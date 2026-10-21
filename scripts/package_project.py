"""Portables Projektarchiv ohne virtuelle Umgebung, Cache oder Messdaten bauen."""

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "output" / "IoT-Realtime-Analysis.zip"
FILES = ("README.md", "pyproject.toml", "requirements.txt", "Dockerfile", "compose.yaml",
         ".env.example", ".gitignore", ".dockerignore")


def main():
    paths = [ROOT / name for name in FILES]
    for directory in ("src", "tests", "scripts", "docs"):
        paths.extend(p for p in (ROOT / directory).rglob("*") if p.is_file()
                     and "__pycache__" not in p.parts and p.suffix not in (".pyc", ".pyo")
                     and not any(part.endswith(".egg-info") for part in p.parts))
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(OUTPUT, "w", compression=ZIP_DEFLATED) as archive:
        for path in sorted(set(paths)):
            archive.write(path, "IoT-Realtime-Analysis/" + path.relative_to(ROOT).as_posix())
    with ZipFile(OUTPUT) as archive:
        bad = archive.testzip()
        if bad is not None:
            raise RuntimeError(f"Fehlerhafter Archiveintrag: {bad}")
        print(f"{OUTPUT} ({len(archive.namelist())} Dateien, {OUTPUT.stat().st_size} Bytes)")


if __name__ == "__main__":
    main()
