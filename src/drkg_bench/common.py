from __future__ import annotations

import csv
import json
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import yaml


ROOT = Path(__file__).resolve().parents[2]


class BenchmarkError(RuntimeError):
    """Project-specific runtime error."""


@dataclass(frozen=True)
class AppContext:
    root: Path
    config_path: Path
    config: dict[str, Any]

    def path(self, value: str | Path) -> Path:
        raw = Path(value)
        if raw.is_absolute():
            return raw
        return self.root / raw

    def ensure_results_dirs(self) -> None:
        for key, value in self.config.get("paths", {}).items():
            if key.endswith("_dir"):
                self.path(value).mkdir(parents=True, exist_ok=True)

    def write_json(self, destination: str | Path, payload: Any) -> None:
        path = self.path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")

    def write_yaml(self, destination: str | Path, payload: Any) -> None:
        path = self.path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(payload, handle, sort_keys=False)

    def write_csv(
        self,
        destination: str | Path,
        fieldnames: list[str],
        rows: Iterable[dict[str, Any]],
    ) -> None:
        path = self.path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)


def load_context(config_path: str | Path) -> AppContext:
    path = Path(config_path)
    if not path.is_absolute():
        path = ROOT / path
    with path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    ctx = AppContext(root=ROOT, config_path=path, config=config)
    ctx.ensure_results_dirs()
    return ctx


def load_yaml(path: str | Path) -> Any:
    path = Path(path)
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def shell_join(command: list[str] | str) -> str:
    if isinstance(command, str):
        return command
    return " ".join(shlex.quote(part) for part in command)


def run_command(
    command: list[str] | str,
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
    capture_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    if isinstance(command, str):
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            shell=True,
            check=False,
            text=True,
            capture_output=capture_output,
        )
    else:
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            shell=False,
            check=False,
            text=True,
            capture_output=capture_output,
        )
    if check and completed.returncode != 0:
        raise BenchmarkError(
            f"Command failed ({completed.returncode}): {shell_join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return completed


def getenv_required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise BenchmarkError(f"Required environment variable is not set: {name}")
    return value


def resolve_secret(
    config: dict[str, Any],
    *,
    env_key: str,
    value_key: str,
    label: str,
) -> tuple[str, str]:
    env_name = config.get(env_key)
    if env_name:
        env_value = os.environ.get(env_name)
        if env_value:
            return env_value, f"env:{env_name}"

    config_value = config.get(value_key)
    if config_value:
        return str(config_value), f"config:{value_key}"

    pieces = []
    if env_name:
        pieces.append(f"env var {env_name}")
    pieces.append(f"config key {value_key}")
    raise BenchmarkError(f"Missing {label}; expected one of: {', '.join(pieces)}")


def print_status(message: str) -> None:
    sys.stderr.write(f"[drkg-bench] {message}\n")
    sys.stderr.flush()
