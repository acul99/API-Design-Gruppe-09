"""Konfiguration ohne zusaetzliche Frameworks."""

from dataclasses import dataclass
import os
import re


@dataclass(frozen=True)
class Settings:
    broker_url: str = "amqp://guest:guest@localhost:5672/%2F"
    namespace: str = "iotlab"
    window_seconds: float = 10.0
    process_attempts: int = 3
    report_seconds: float = 5.0

    def __post_init__(self):
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", self.namespace):
            raise ValueError("IOT_NAMESPACE: 1-64 Buchstaben, Ziffern, _ oder - erforderlich")
        if self.window_seconds <= 0 or self.process_attempts < 1 or self.report_seconds <= 0:
            raise ValueError("Zeitintervalle und Versuchsanzahl muessen positiv sein")

    @classmethod
    def from_env(cls):
        return cls(
            broker_url=os.getenv("AMQP_URL", cls.broker_url),
            namespace=os.getenv("IOT_NAMESPACE", cls.namespace),
        )
