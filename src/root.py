from pathlib import Path

def __get_project_root() -> Path:
    current_path = Path(__file__).resolve().parent
    while current_path != current_path.parent:
        if (current_path / "pyproject.toml").exists():
            return current_path
        current_path = current_path.parent
    raise FileNotFoundError("Cannot find project root - no root markers found.")

ROOT_PATH = __get_project_root()
