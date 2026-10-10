import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from threading import Lock, RLock
from typing import Any, Generator

from scout.models import Lead


class LeadStore:
    """Thread-safe in-memory store; data is cleared when the process exits."""

    def __init__(self) -> None:
        self._leads: dict[str, Lead] = {}
        self._lock = Lock()

    def save(self, lead: Lead) -> None:
        with self._lock:
            self._leads[lead.id] = lead

    def get(self, lead_id: str) -> Lead | None:
        with self._lock:
            return self._leads.get(lead_id)


class V2StateStore:
    """Durable JSON state for V2 task progress and normalized leads."""

    def __init__(self, path: str | Path | None = None) -> None:
        default_path = Path.home() / ".auren-scout-v2" / "state.json"
        self.path = Path(path or os.getenv("SCOUT_V2_STATE_PATH") or default_path)
        self._lock = RLock()
        self._state: dict[str, Any] = {"tasks": {}, "leads": {}}
        self._reload()

    def _reload(self) -> None:
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise RuntimeError(f"Não foi possível ler o estado V2: {self.path}") from error
            if (
                not isinstance(loaded, dict)
                or not isinstance(loaded.get("tasks"), dict)
                or not isinstance(loaded.get("leads"), dict)
            ):
                raise RuntimeError(f"Formato inválido no estado V2: {self.path}")
            self._state = loaded

    def get_task_record(self, task_id: str) -> dict[str, Any] | None:
        with self._lock, self._state_file_lock():
            self._reload()
            record = self._state["tasks"].get(task_id)
            return self._copy_json(record) if isinstance(record, dict) else None

    def task_exists(self, task_id: str) -> bool:
        with self._lock, self._state_file_lock():
            self._reload()
            return task_id in self._state["tasks"]

    def create_task_record(self, task_id: str, record: dict[str, Any]) -> bool:
        with self._lock, self._state_file_lock():
            self._reload()
            if task_id in self._state["tasks"]:
                return False
            self._state["tasks"][task_id] = self._copy_json(record)
            self._write()
            return True

    def load_task_records(self) -> dict[str, dict[str, Any]]:
        with self._lock, self._state_file_lock():
            self._reload()
            return self._copy_json(self._state["tasks"])

    def load_leads(self) -> list[dict[str, Any]]:
        with self._lock, self._state_file_lock():
            self._reload()
            return self._copy_json(list(self._state["leads"].values()))

    def save_task_record(self, task_id: str, record: dict[str, Any]) -> None:
        with self._lock, self._state_file_lock():
            self._reload()
            self._state["tasks"][task_id] = self._copy_json(record)
            self._write()

    def replace_leads(self, leads: list[dict[str, Any]]) -> None:
        indexed: dict[str, dict[str, Any]] = {}
        for lead in leads:
            lead_id = lead.get("lead_id") if isinstance(lead, dict) else None
            if not isinstance(lead_id, str) or not lead_id:
                raise ValueError("Cada lead persistido deve ter lead_id")
            indexed[lead_id] = self._copy_json(lead)
        with self._lock, self._state_file_lock():
            self._reload()
            self._state["leads"] = indexed
            self._write()

    @contextmanager
    def _state_file_lock(self) -> Generator[None, None, None]:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock_path = self.path.with_name(f"{self.path.name}.lock")
        with open(lock_path, "a+b") as lock_file:
            if hasattr(os, "chmod"):
                os.chmod(lock_path, 0o600)
            lock_file.seek(0, os.SEEK_END)
            if lock_file.tell() == 0:
                lock_file.write(b"\0")
                lock_file.flush()
            lock_file.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
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

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary_file:
                temporary_path = temporary_file.name
                if hasattr(os, "chmod"):
                    os.chmod(temporary_path, 0o600)
                json.dump(self._state, temporary_file, ensure_ascii=False, separators=(",", ":"))
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            os.replace(temporary_path, self.path)
            temporary_path = None
        finally:
            if temporary_path and os.path.exists(temporary_path):
                os.unlink(temporary_path)

    @staticmethod
    def _copy_json(value: Any) -> Any:
        return json.loads(json.dumps(value, ensure_ascii=False))