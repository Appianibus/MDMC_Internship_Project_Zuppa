from pathlib import Path

from application.utils import load_config

config = load_config()


class VersionObject:
    def __init__(self, pipeline_version: str, software_version: str):

        self.pipeline_version = pipeline_version
        self.software_version = software_version


BASE_DIR = Path(__file__).resolve().parent.parent.parent
REGISTRIES_DIR = BASE_DIR / config["paths"]["registries"]
SUBREGISTRIES_DIR = BASE_DIR / config["paths"]["subregistries"]
RECORDS_DIR = BASE_DIR / config["paths"]["records"]


def repo_relative_path(path: str | Path) -> str:
    path = Path(path)

    try:
        return path.resolve().relative_to(BASE_DIR).as_posix()
    except ValueError:
        return str(path)


def resolve_registry_path(path: str | Path) -> Path:
    path = Path(path)

    if path.is_absolute():
        return path

    return BASE_DIR / path
