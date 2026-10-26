"""Alle fuenf Prozesse starten. Strg+C beendet sie gemeinsam."""

from pathlib import Path
import subprocess
import sys
import time

from common import close, open_channel


def main():
    connection, _ = open_channel()
    close(connection)
    root = Path(__file__).resolve().parent
    processes = []
    try:
        for command in (("analysis.py", "environment"), ("analysis.py", "machinery"),
                        ("alarm.py",), ("monitor.py",), ("producer.py",)):
            processes.append(subprocess.Popen([sys.executable, "-u", *command], cwd=root,
                             creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0)))
        print("System gestartet. Mit Strg+C beenden.", flush=True)
        while True:
            if any(process.poll() is not None for process in processes):
                raise RuntimeError("Ein Prozess wurde beendet; siehe dessen Fehlermeldung.")
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            process.wait(timeout=5)


if __name__ == "__main__":
    main()
