"""Alle fuenf Prozesse starten. Strg+C beendet sie gemeinsam."""

from pathlib import Path
import subprocess
import sys
from threading import Thread
import time

from common import close, open_channel


def show_output(process):
    """Ausgaben eines Kindprozesses im Terminal des Starters anzeigen."""
    with process.stdout:
        for line in process.stdout:
            print(line, end="", flush=True)


def main():
    connection, _ = open_channel()
    close(connection)
    root = Path(__file__).resolve().parent
    processes = []
    output_threads = []
    try:
        for command in (("analysis.py", "environment"), ("analysis.py", "machinery"),
                        ("alarm.py",), ("monitor.py",), ("producer.py",)):
            process = subprocess.Popen([sys.executable, "-u", "-X", "utf8", *command], cwd=root,
                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8", errors="replace",
                      creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            processes.append(process)
            output_thread = Thread(target=show_output, args=(process,), daemon=True)
            output_thread.start()
            output_threads.append(output_thread)
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
        for output_thread in output_threads:
            output_thread.join(timeout=1)


if __name__ == "__main__":
    main()
