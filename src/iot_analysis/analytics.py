"""Kleine, transportunabhängige Bausteine für Zeitfenster und Monitoring."""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass, field
import math
import time
from typing import Any


@dataclass
class TemperatureWindow:
    """Zusammenhängende Empfangszeitfenster, gemessen mit monotoner Uhr.

    ``pending`` enthält die noch nicht bestätigten Lieferungen. Sie werden erst
    nach Veröffentlichung des Fensterergebnisses bestätigt. Ein Sensorzeitstempel
    bestimmt bewusst nicht das Fenster: Ausschlaggebend ist der Empfang.
    """

    seconds: float = 10.0
    started_at: float = field(default_factory=time.monotonic)
    values: list[float] = field(default_factory=list)
    pending: list[Any] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not math.isfinite(self.seconds) or self.seconds <= 0:
            raise ValueError("Die Fensterlänge muss positiv sein.")

    def due(self, now: float | None = None) -> bool:
        return (time.monotonic() if now is None else now) >= self.started_at + self.seconds

    def add(self, value: float, pending: Any) -> None:
        self.values.append(value)
        self.pending.append(pending)

    def summary(self) -> dict[str, int | float] | None:
        if not self.values:
            return None
        return {
            "count": len(self.values),
            "average": math.fsum(value / len(self.values) for value in self.values),
            "min": min(self.values),
            "max": max(self.values),
            "window_seconds": self.seconds,
        }

    def advance(self, now: float | None = None) -> None:
        """Leert das Fenster und überspringt bei Leerlauf nur leere Intervalle."""
        current = time.monotonic() if now is None else now
        elapsed = max(1, int((current - self.started_at) // self.seconds))
        self.started_at += elapsed * self.seconds
        self.values.clear()
        self.pending.clear()


class MonitoringStats:
    """Zählt ausschließlich empfangene Telemetrie, mit begrenzter Duplikaterkennung."""

    def __init__(self, dedup_limit: int = 10_000) -> None:
        if dedup_limit < 1:
            raise ValueError("Die Duplikat-Historie muss mindestens einen Eintrag behalten.")
        self.processed = 0
        self.per_type: Counter[str] = Counter()
        self.anomalies = 0
        self.failures = 0
        self.processing_ms = 0.0
        self._seen: set[str] = set()
        self._order: deque[str] = deque()
        self._dedup_limit = dedup_limit

    def observe(self, event: dict[str, Any]) -> bool:
        """Gibt bei bereits bekanntem event_id False zurück."""
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("Telemetrie benötigt eine event_id.")
        if event_id in self._seen:
            return False
        kind = event.get("kind")
        if not isinstance(kind, str):
            raise ValueError("Telemetrie benötigt kind.")
        if kind == "sensor_processed":
            sensor_type = event.get("sensor_type", event.get("type"))
            duration = event.get("processing_ms", 0.0)
            if not isinstance(sensor_type, str) or not sensor_type:
                raise ValueError("sensor_processed benötigt sensor_type.")
            if (isinstance(duration, bool) or not isinstance(duration, (int, float))
                    or not math.isfinite(duration) or duration < 0):
                raise ValueError("processing_ms muss eine nichtnegative Zahl sein.")
            self.processed += 1
            self.per_type[sensor_type] += 1
            self.anomalies += int(bool(event.get("alarm")))
            self.processing_ms += duration
        elif kind == "processing_failed":
            self.failures += 1
        self._seen.add(event_id)
        self._order.append(event_id)
        if len(self._order) > self._dedup_limit:
            self._seen.remove(self._order.popleft())
        return True

    def snapshot(self) -> dict[str, Any]:
        return {
            "processed": self.processed,
            "per_type": dict(sorted(self.per_type.items())),
            "anomalies": self.anomalies,
            "failures": self.failures,
            "avg_processing_ms": round(self.processing_ms / self.processed, 3) if self.processed else 0.0,
        }
