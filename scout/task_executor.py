from __future__ import annotations

import errno
import logging
import os
import re
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from hashlib import sha256
from threading import RLock
from typing import Any, Generator

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from scout.aivio import AivioIntegration, CDP_ENDPOINT
from scout.browser_controller import BrowserController
from scout.lead_normalization import deduplicate_leads
from scout.storage import V2StateStore
from scout.v2_models import (
    LeadTaskPayload,
    SearchLeadsPayload,
    Task,
    TaskResult,
    TaskStatus,
    TaskType,
    V2Lead,
)

logger = logging.getLogger("scout.v2.tasks")
_BROWSER_TASK_LOCK = RLock()
_TASK_SECRET_PATTERNS = (
    re.compile(r"(?i)(https?://)[^/\s:@]+:[^@\s/]+@"),
    re.compile(r"(?i)bearer\s+[a-z0-9._~+/=-]+"),
    re.compile(r"\beyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\b"),
    re.compile(
        r"""(?i)(?:access[_ -]?token|refresh[_ -]?token|csrf(?:[_ -]?token)?|"""
        r"""authorization|cookie|password|secret|api[_ -]?key)\s*[:=]\s*"""
        r"""["']?[^\s,;"'}&#]+"""
    ),
)


def _safe_error_message(error: Exception) -> str:
    message = f"{type(error).__name__}: {error}"
    for pattern in _TASK_SECRET_PATTERNS:
        message = pattern.sub("[REDACTED]", message)
    return message


@contextmanager
def _browser_execution_lock(endpoint: str) -> Generator[None, None, None]:
    lock_name = sha256(endpoint.encode("utf-8")).hexdigest()[:16]
    lock_dir = tempfile.gettempdir()
    if hasattr(os, "getuid"):
        lock_dir = os.path.join(lock_dir, f"auren-scout-{os.getuid()}")
        os.makedirs(lock_dir, mode=0o700, exist_ok=True)
        directory_stat = os.stat(lock_dir)
        if directory_stat.st_uid != os.getuid() or directory_stat.st_mode & 0o077:
            raise PermissionError(f"Diretório de lock não é privado: {lock_dir}")
    lock_path = os.path.join(lock_dir, f"auren-scout-{lock_name}.lock")
    with _BROWSER_TASK_LOCK, open(lock_path, "a+b") as lock_file:
        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write(b"\0")
            lock_file.flush()
        lock_file.seek(0)
        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError as error:
                    if error.errno not in (errno.EACCES, errno.EDEADLK):
                        raise
                    time.sleep(0.05)
            try:
                yield
            finally:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


class TaskExecutor:
    def __init__(
        self,
        integration: AivioIntegration | None = None,
        browser_controller: BrowserController | None = None,
        state_store: V2StateStore | None = None,
    ) -> None:
        timeout = int(os.getenv("SCOUT_AIVIO_GENERATION_TIMEOUT_MS", "120000"))
        if integration is None:
            browser_controller = browser_controller or BrowserController(
                cdp_endpoint=os.getenv("SCOUT_CDP_ENDPOINT", CDP_ENDPOINT)
            )
            integration = AivioIntegration(browser_controller, generation_timeout_ms=timeout)
        self.integration = integration
        self.browser_controller = browser_controller or getattr(integration, "browser_controller", None)
        self._browser_endpoint = (
            getattr(self.browser_controller, "cdp_endpoint", None) or CDP_ENDPOINT
        )
        self.state_store = state_store or V2StateStore()
        self._task_results: dict[str, TaskResult] = {}
        self._task_results_lock = RLock()
        for task_id, record in self.state_store.load_task_records().items():
            result = self._result_from_dict(record)
            if (
                result.status in {TaskStatus.QUEUED, TaskStatus.RUNNING}
                and not self._process_is_alive(result.worker_pid)
            ):
                result.status = TaskStatus.FAILED
                result.error = {
                    "code": "task_interrupted",
                    "message": "A tarefa foi interrompida antes da conclusão; não foi repetida automaticamente.",
                }
                result.errors.append(result.error["message"])
                result.finished_at = datetime.now(timezone.utc).isoformat()
                self.state_store.save_task_record(task_id, result.to_dict())
            self._task_results[task_id] = result

    def execute(self, value: Task | dict[str, Any]) -> TaskResult:
        task = value if isinstance(value, Task) else Task.from_dict(value)
        queued_result = TaskResult(
            task_id=task.task_id,
            type=task.type,
            status=TaskStatus.QUEUED,
            started_at="",
            finished_at="",
            worker_pid=os.getpid(),
        )
        with self._task_results_lock:
            if (
                task.task_id in self._task_results
                or not self.state_store.create_task_record(task.task_id, queued_result.to_dict())
            ):
                raise ValueError(f"task_id já foi executado: {task.task_id}")
            self._task_results[task.task_id] = queued_result
        try:
            with _browser_execution_lock(self._browser_endpoint):
                with self._task_results_lock:
                    queued_result.status = TaskStatus.RUNNING
                    queued_result.started_at = datetime.now(timezone.utc).isoformat()
                    self._persist_result(queued_result)
                result = self._execute_task(task, queued_result)
                with self._task_results_lock:
                    result.finished_at = datetime.now(timezone.utc).isoformat()
                    self._persist_result(result)
                return result
        except Exception as error:
            message = _safe_error_message(error)
            logger.error("Falha ao iniciar tarefa V2 task_id=%s error=%s", task.task_id, message)
            with self._task_results_lock:
                queued_result.status = TaskStatus.FAILED
                queued_result.errors.append(message)
                queued_result.error = {"code": "task_start_failed", "message": message}
                queued_result.finished_at = datetime.now(timezone.utc).isoformat()
                self._persist_result(queued_result)
            return queued_result

    def get_task_result(self, task_id: str) -> dict[str, Any] | None:
        with self._task_results_lock:
            result = self._task_results.get(task_id)
            persisted = self.state_store.get_task_record(task_id)
            if persisted is not None:
                result = self._result_from_dict(persisted)
                self._task_results[task_id] = result
            if result is None:
                return None
            if result.finished_at:
                return result.to_dict()
            status = TaskStatus.QUEUED if result.status == TaskStatus.QUEUED else TaskStatus.RUNNING
            snapshot = TaskResult(
                task_id=result.task_id,
                type=result.type,
                status=status,
                progress=dict(result.progress),
                started_at=result.started_at,
                finished_at="",
            )
            return snapshot.to_dict()

    def _execute_task(self, task: Task, result: TaskResult) -> TaskResult:
        try:
            if task.type == TaskType.HEALTH_CHECK:
                result.data = self._read_health()
            else:
                self._connect_for_task()
                if task.type == TaskType.SEARCH_LEADS:
                    self._search_leads(task, result)
                elif task.type == TaskType.OPEN_COMPANY:
                    self._open_company(task, result)
                elif task.type == TaskType.GENERATE_SITE:
                    self._generate_site(task, result)
                else:
                    raise ValueError(f"Tipo de tarefa não suportado: {task.type}")
        except Exception as error:
            message = _safe_error_message(error)
            logger.error(
                "Falha ao executar tarefa V2 task_id=%s type=%s error=%s",
                task.task_id,
                task.type.value,
                message,
            )
            result.errors.append(message)
            if isinstance(error, (PlaywrightTimeoutError, TimeoutError)):
                result.status = TaskStatus.TIMEOUT
                result.error = {"code": "task_timeout", "message": message}
            else:
                result.status = TaskStatus.FAILED
                result.error = {"code": "task_failed", "message": message}
        else:
            if result.status == TaskStatus.RUNNING:
                result.status = TaskStatus.COMPLETED
        return result

    def health_check(self) -> dict[str, Any]:
        with _browser_execution_lock(self._browser_endpoint):
            return self._read_health()

    def _read_health(self) -> dict[str, Any]:
        health_reader = getattr(self.integration, "browser_health", None)
        if health_reader is None:
            return {
                "browser_connected": False,
                "edge_connected": False,
                "edge_product": None,
                "browser_version": None,
                "active_tab": None,
                "aivio_available": False,
                "aivio_hostname": None,
                "aivio_url": None,
                "cdp_endpoint": self._browser_endpoint,
                "expected_edge_profile": "EdgeProfile-V2",
                "operational": False,
                "browser_error": "O adaptador de browser não oferece health check.",
            }
        health = health_reader()
        if not isinstance(health, dict):
            raise RuntimeError("O health check retornou um formato inválido")
        health = dict(health)
        if isinstance(health.get("browser_error"), str):
            health["browser_error"] = _safe_error_message(RuntimeError(health["browser_error"]))
        return health

    def _connect_for_task(self) -> None:
        connect = getattr(self.browser_controller, "connect", None)
        if connect is not None:
            connect()

    def _persist_result(self, result: TaskResult) -> None:
        self.state_store.save_task_record(result.task_id, result.to_dict())

    def _update_progress(self, result: TaskResult, progress: dict[str, Any]) -> None:
        with self._task_results_lock:
            result.progress = dict(progress)
            self._persist_result(result)

    @staticmethod
    def _result_from_dict(record: dict[str, Any]) -> TaskResult:
        try:
            result = TaskResult(
                task_id=record["task_id"],
                type=TaskType(record["type"]),
                status=TaskStatus(record["status"]),
                data=record.get("data", {}),
                leads=[V2Lead.from_dict(item) for item in record.get("leads", [])],
                artifacts=record.get("artifacts", []),
                errors=record.get("errors", []),
                warnings=record.get("warnings", []),
                progress=record.get("progress", {}),
                worker_pid=record.get("worker_pid"),
                error=record.get("error"),
                started_at=record.get("started_at", ""),
                finished_at=record.get("finished_at", ""),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise RuntimeError("Registro persistido de tarefa V2 inválido") from error
        return result

    @staticmethod
    def _process_is_alive(process_id: int | None) -> bool:
        if isinstance(process_id, bool) or not isinstance(process_id, int) or process_id < 1:
            return False
        try:
            os.kill(process_id, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _search_leads(self, task: Task, result: TaskResult) -> None:
        payload = task.payload
        if not isinstance(payload, SearchLeadsPayload):
            raise TypeError("SEARCH_LEADS exige SearchLeadsPayload")
        filters = payload.filters
        warnings = [
            f"Filtro não suportado e não aplicado: {name}"
            for name in filters if name != "has_website"
        ]
        search = self.integration.search_leads(
            city=payload.city,
            state=payload.state,
            category=payload.category,
            quantity=payload.quantity,
            filters=filters,
            allow_credit_consumption=task.authorization.get("allow_credit_consumption") is True,
            on_progress=lambda progress: self._update_progress(result, progress),
        )
        known_leads = [V2Lead.from_dict(item) for item in self.state_store.load_leads()]
        batch_leads = [V2Lead.from_dict(item) for item in search["results"]]
        known_ids = {lead.lead_id for lead in known_leads}
        combined_leads = deduplicate_leads(known_leads + batch_leads)
        self.state_store.replace_leads([lead.to_dict() for lead in combined_leads])
        result.leads = [lead for lead in combined_leads if lead.lead_id not in known_ids]
        duplicate_count = max(0, len(batch_leads) - len(result.leads))
        if duplicate_count:
            result.warnings.append(
                f"{duplicate_count} lead(s) já existentes foram ignorados neste lote."
            )
        result.warnings.extend(warnings)
        result.warnings.extend(search.get("warnings", []))
        result.data = {
            "city": search["city"],
            "state": search["state"],
            "category": search["category"],
            "requested_quantity": search["requested_quantity"],
            "total": len(result.leads),
            "unique_total": len(combined_leads),
            "duplicates_skipped": duplicate_count,
        }
        if search["status"] == "partial" or warnings or duplicate_count:
            result.status = TaskStatus.PARTIAL

    def _open_company(self, task: Task, result: TaskResult) -> None:
        payload = task.payload
        if not isinstance(payload, LeadTaskPayload):
            raise TypeError(f"{task.type.value} exige LeadTaskPayload")
        updated = self.integration.open_company(
            payload.lead,
            allow_credit_consumption=task.authorization.get("allow_credit_consumption") is True,
        )
        result.leads = [updated]
        result.data = {"lead": updated.to_dict()}

    def _generate_site(self, task: Task, result: TaskResult) -> None:
        payload = task.payload
        if not isinstance(payload, LeadTaskPayload):
            raise TypeError("GENERATE_SITE exige LeadTaskPayload")
        updated, artifacts, warnings = self.integration.generate_site(
            payload.lead,
            allow_credit_consumption=task.authorization.get("allow_credit_consumption") is True,
            allow_external_effects=task.authorization.get("allow_external_effects") is True,
        )
        result.leads = [updated]
        result.data = {"lead": updated.to_dict()}
        result.artifacts = artifacts
        result.warnings.extend(warnings)
        website = next((item.get("url") for item in artifacts if item.get("type") == "website"), None)
        if isinstance(website, str):
            updated.website = website
            updated.has_website = True
            result.data["lead"] = updated.to_dict()
        if warnings:
            result.status = TaskStatus.PARTIAL
