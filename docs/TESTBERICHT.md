# Tatsächlich ausgeführte Prüfungen

Stand: **21. Oktober 2026**, lokal unter Windows. Alle hier als bestanden bezeichneten
Prüfungen wurden ausgeführt; es handelt sich nicht nur um geplante Szenarien.

## Umgebung

- Python **3.12.14**, virtuelle Umgebung `.venv`
- Pika **1.3.2**, installiert über `pip install -e .`
- RabbitMQ **4.4.0**, Version aus den AMQP-Servereigenschaften ausgelesen
- Einzelner lokal laufender Broker; für jeden Test ein neuer UUID-Namespace

Der vorhandene Broker wurde nicht angehalten oder umkonfiguriert. Die Tests
entfernten anschließend ausschließlich ihre selbst erstellten Exchanges und Queues.
Worker-Logs liegen lokal unter `.state/test_…/`. Das ZIP enthält diese temporären
Mess- und Prozessdaten nicht.

## Ergebnisse

| Prüfung | Beobachtetes Ergebnis |
| --- | --- |
| Vollständige Unit-Suite | **57 Tests bestanden**: Verträge, Alarmgrenzen, Routing, TTL-Konfiguration, Priorität, Zahlenvalidierung, Zeitfenster, Monitoring, Producer-Retries und Consumer-ACK/NACK |
| Echter regulärer Producer | Sechs persistente Messungen, davon vier in Umgebung und zwei in Maschine; jeder der drei Typen zweimal; `delivery_mode=2` und IDs geprüft |
| Vollständige Verarbeitungskette | Neun Messungen, drei je Typ, sechs Alarme; separater Monitor zählt alle neun korrekt |
| Zehn-Sekunden-Fenster | Drei Temperaturen 20, 85 und 110; Anzahl 3, Mittelwert 71,666…; Fensterabschluss auch ohne weitere Nachricht |
| Ungültiges JSON und fehlender Messwert | Zwei echte DLQ-Nachrichten, `x-death.reason=rejected`; danach normale Verarbeitung; Monitor zählt zwei Fehler |
| Lokale Verarbeitungsfehler | Transienter Fehler gelingt im zweiten Versuch; permanenter Fehler endet nach drei Versuchen in DLQ |
| Echter Consumer-Prozessabbruch | Empfang vor ACK, Prozess beendet, Ersatzconsumer verarbeitet dieselbe ID mit `redelivered=true`; kein Dead Letter allein durch den Crash |
| Brokerseitige Alarmpriorität | Wartende Alarme werden als CRITICAL, CRITICAL, WARNING, WARNING, WARNING verarbeitet |
| Kurze Nachrichten-TTL | Zusätzliche TTL von 1 s führt in DLQ mit `reason=expired` |
| Echte Hauptqueue-TTL | Ohne zusätzliche Nachrichten-TTL: tatsächlicher Ablauf nach 60 s; DLQ-Ursache `expired` |
| Dokumentierte CLI | `check` sowie `demo --scenario all` mit allen fünf Szenarien erfolgreich beendet |
| Präsentation | Zehn PDF-Folien erzeugt, gerendert und visuell geprüft; 15-Minuten-Vortragsplan inklusive vier Minuten Demo |

Die sieben ursprünglichen Integrationstests einschließlich des echten
60-Sekunden-Tests bestanden in **74,887 s**. Nach Ergänzung des regulären
Producer-Tests und der Monitoring-Fehlerprüfung bestand der kurze Integrationslauf
in **20,369 s**: sieben Tests bestanden, nur der bereits separat erfolgreich
ausgeführte 60-Sekunden-Test war übersprungen. Insgesamt sind damit **acht
unterschiedliche Broker-Integrationstests** erfolgreich geprüft.

Der anschließende dokumentierte CLI-Lauf aller fünf Demos dauerte **8,865 s**
und endete mit Exitcode 0. Seine Ausgabe enthielt unter anderem:

```text
invalid:    dlq=2, reason=rejected, monitoring_failures=2
processing: transient_attempts=2, permanent_attempts=3, dlq=1
crash:      ack_before_crash=false, redelivered=true, dlq=0
priority:   CRITICAL, CRITICAL, WARNING, WARNING, WARNING
ttl:        reason=expired, elapsed_seconds=1.02, message_ttl_ms=1000
```

Diese gekürzte Darstellung stammt aus den tatsächlichen `demo_passed`-Ausgaben.
Die Demos melden Erfolg erst nach Assertions über den beobachteten Brokerzustand
und die Ausgaben der separat gestarteten Worker.

## Reproduktion

Im aktivierten Projekt-Python:

```powershell
python -m unittest discover -s tests -v
$env:RUN_RABBITMQ_TESTS = '1'
$env:RUN_FULL_TTL_TEST = '1'
python -m unittest discover -s tests/integration -v
python -m iot_analysis demo --scenario all
```

Ohne `RUN_RABBITMQ_TESTS=1` werden die Brokerprüfungen bewusst übersprungen.
Ohne `RUN_FULL_TTL_TEST=1` wird innerhalb des Brokerlaufs nur der lange
60-Sekunden-Test übersprungen. Unit-Tests benötigen keinen laufenden Broker.

## Prüfgrenzen

Docker ist auf diesem Rechner nicht verfügbar. Dockerfile und Compose-Konfiguration
sind vorbereitet und strukturell geprüft; ein tatsächlicher Containerstart wurde
hier nicht durchgeführt. Die Anwendung wurde mit dem oben genannten lokalen
RabbitMQ getestet, nicht mit dem in Compose festgelegten RabbitMQ 4.3.6.

Die Tests behaupten weder Hochverfügbarkeit noch globale Exactly-once-Verarbeitung.
Duplikate zwischen Publish und Quell-ACK sowie flüchtige Monitoringstände bleiben
die in `ARCHITEKTUR.md` beschriebenen Grenzen des Prototyps.
