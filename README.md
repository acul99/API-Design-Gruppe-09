# IoT-Sensordaten mit RabbitMQ

Ein kleines Vorlesungsprojekt mit Python und Pika: Drei simulierte Sensortypen liefern Messwerte. Zwei Analyse-Consumer erkennen Anomalien, ein Alarm-Consumer verarbeitet priorisierte Alarme und ein Monitoring-Consumer zeigt Kennzahlen. Die Anwendung besteht aus sieben Python-Dateien.

## Start

Voraussetzungen: Python ab 3.11 und ein laufender RabbitMQ auf `localhost:5672`.

```powershell
python -m pip install -r requirements.txt
python start.py
```

In der bereits eingerichteten Projektumgebung auf diesem Rechner genügt:

```powershell
.\.venv\Scripts\python.exe start.py
```

`start.py` startet fünf Prozesse: einen Producer, zwei Analysen, Alarm und Monitoring. Nach etwa zehn Sekunden erscheint das erste Temperaturaggregat. `Strg+C` beendet die gestarteten Prozesse. Der Broker und seine Nachrichten bleiben erhalten.

Falls kein Broker installiert ist, stellt optional Docker Compose ausschließlich RabbitMQ bereit:

```powershell
docker compose up -d
```

Ein bereits laufender lokaler Broker reicht aus. Für einen anderen Broker die Verbindungsadresse setzen:

```powershell
$env:AMQP_URL = 'amqp://benutzer:passwort@localhost:5672/%2F'
python start.py
```

Standard ist `amqp://guest:guest@localhost:5672/%2F`. Das Konto `guest` ist für lokale Verbindungen vorgesehen.

## Die sieben Dateien

| Datei | Aufgabe |
| --- | --- |
| `common.py` | Verbindung, JSON-Prüfung, Queue-Deklaration und Publikation |
| `producer.py` | Temperatur, Luftfeuchtigkeit und Vibration simulieren |
| `analysis.py` | Umgebung oder Maschine analysieren, Alarmregeln und Temperaturfenster |
| `alarm.py` | WARNING und CRITICAL nach Brokerpriorität verarbeiten |
| `monitor.py` | Über RabbitMQ empfangene Kennzahlen ausgeben |
| `demo.py` | Zwei echte, isolierte Fehlerszenarien durchführen |
| `start.py` | Alle Dienste starten und gemeinsam beenden |

Zum Erklären lassen sich die Dienste auch einzeln in fünf Terminals starten:

```text
python producer.py
python analysis.py environment
python analysis.py machinery
python alarm.py
python monitor.py
```

Jeder Dienst deklariert die benötigte Topologie automatisch. Ein separates Setup-Kommando ist nicht erforderlich.

## Nachrichtenfluss mit zwei Exchanges

| Exchange | Typ | Routing Key / Binding | Zielqueue |
| --- | --- | --- | --- |
| `iot.sensors` | topic | `sensors.temperature.*`, `sensors.humidity.*` | `iot.environment` |
| `iot.sensors` | topic | `sensors.vibration.*` | `iot.machinery` |
| `iot.events` | direct | `WARNING`, `CRITICAL` | `iot.alarms` |
| `iot.events` | direct | `monitor` | `iot.monitor` |
| `iot.events` | direct | `dead` | `iot.dlq` |

Topic filtert nach Sensortyp bereits im Broker. Direct reicht für die wenigen festen Ereignisschlüssel. Ein Fanout-Exchange für Sensoren würde unnötige Kopien an beide Analysen senden. Alarme, Monitoring und Dead Letters teilen einen Direct-Exchange, besitzen aber getrennte Queues.

Eine Messung enthält direkt die geforderten JSON-Felder:

```json
{"sensor_id":"temp-01","timestamp":"2026-10-26T10:00:00Z","type":"temperature","value":92.5}
```

Die Simulation verwendet °C, relative Luftfeuchtigkeit in Prozent und Vibration in mm/s. Temperatur über 80/100 erzeugt WARNING/CRITICAL, Luftfeuchtigkeit außerhalb 30-70/15-85 und Vibration über 5/10 entsprechend. Die strengere Alarmstufe hat Vorrang. Diese Grenzwerte sind Annahmen für die Demonstration.

## Zuverlässigkeit und Vorlesungsbegriffe

- **Persistenz:** Durable Exchanges/Queues und Nachrichten mit `delivery_mode=2`. Publisher Confirms bestätigen die Brokerannahme, `mandatory=True` macht fehlendes Routing sichtbar.
- **Exponential Backoff:** Nach dem ersten Versuch höchstens fünf Producer-Wiederholungen mit 1, 2, 4, 8 und 10 Sekunden Pause.
- **Wiederverbindung:** Analyse, Alarm und Monitoring verbinden sich nach vorübergehenden Verbindungsfehlern mit 1, 2, 4, 8 und danach höchstens 10 Sekunden Pause erneut. Sie bleiben währenddessen aktiv, damit der Starter die Producer-Retries nicht vorzeitig abbricht. Nach einer mindestens 60 Sekunden stabilen Sitzung beginnt die Wartefolge wieder bei 1 Sekunde. Dauerhafte Zugriffs-, Konfigurations- und Programmfehler werden nicht wiederholt. Bei ausgeschöpften Producer-Retries beendet der Starter weiterhin das Gesamtsystem. Neue Consumer-Sitzungen beginnen mit leeren Temperaturfenstern bzw. Monitoringzählern.
- **Nachrichtenprüfung:** Vor der Verarbeitung wird auch die vollständige weiterzugebende JSON-Nachricht geprüft. Nicht endliche Zahlen in Zusatzfeldern und Nachrichten-IDs mit mehr als 255 UTF-8-Bytes werden ohne Requeue abgelehnt und gelangen in die DLQ.
- **Manuelle ACKs:** Erst nach erfolgreicher Verarbeitung und bestätigter Alarm-/Monitoringpublikation bestätigen. Temperaturwerte bleiben bis zum abgeschlossenen Zehn-Sekunden-Fenster unbestätigt.
- **Aggregation:** Alle innerhalb eines zusammenhängenden Zehn-Sekunden-Fensters empfangenen gültigen Temperaturen ergeben Anzahl, Mittelwert, Minimum und Maximum. Ein Timer schließt das Fenster auch ohne neue Nachricht ab.
- **Priorität:** Alarmqueue mit `x-max-priority=9`, WARNING mit Priorität 3, CRITICAL mit 9. Prefetch 1 begrenzt vorab ausgelieferte Alarme. Bereits laufende Verarbeitung wird nicht unterbrochen.
- **TTL und DLQ:** Hauptqueues haben `x-message-ttl=60000` und leiten abgelaufene oder mit `requeue=False` abgelehnte Nachrichten über `iot.events` / `dead` an die DLQ. Die DLQ hat keine TTL und keine automatische Rückführung.
- **Monitoring:** Ein eigener Consumer zählt verarbeitete Messungen, Werte je Typ, Alarme, Fehler und mittlere Verarbeitungszeit anhand eigener RabbitMQ-Nachrichten.

Diese Begriffe stammen hauptsächlich aus Kapitel 3 der Vorlesung. Kapitel 2 erklärt die lose gekoppelte, asynchrone Kommunikation. JSON und Python/Pika/RabbitMQ entsprechen der Aufgabenstellung. gRPC ist für diesen Nachrichtenfluss nicht erforderlich.

## Zwei reproduzierbare Fehlerszenarien

```powershell
python demo.py invalid
python demo.py crash
```

Die Demos verwenden eigene Queuenamen und entfernen anschließend nur ihre eigenen RabbitMQ-Ressourcen.

| Szenario | Auslöser | ACK/NACK und Nachrichtenweg | Danach |
| --- | --- | --- | --- |
| `invalid` | Gezielt ungültige Sensornachricht | NACK ohne Requeue; tatsächlicher DLQ-Eintrag mit `x-death.reason=rejected` | Gültige Nachricht wird wieder verarbeitet |
| `crash` | Analyseprozess während der Verarbeitung beenden | Weder ACK noch NACK; Broker stellt unbestätigte Nachricht nach Neustart erneut zu | Ersatzconsumer verarbeitet und bestätigt sie |

Ein Consumer-Absturz allein führt bei Classic-Queues zunächst zur erneuten Zustellung. Die DLQ greift bei expliziter Ablehnung ohne Requeue oder beim Ablauf wartender Nachrichten. Diese Präzisierung ist für die Erklärung der Vorlesungsfolie zu DLQ-Auslösern wichtig. Ungültige Nachrichten werden sofort abgelehnt und durchlaufen keine endlose Requeue-Schleife.

Tests für die kleine Logik ausführen:

```powershell
python -m unittest discover -s tests -v
```

Mit `$env:RUN_RABBITMQ_TESTS = '1'` werden die beiden Fehlerdemos zusätzlich im Test ausgeführt; dafür muss RabbitMQ laufen.

Am 26.10.2026 lokal geprüft: sechs Logiktests, beide Fehlerdemos, Gesamtfluss mit Aggregation und Monitoring, Alarmpriorität, Start/Stop und echte 60-Sekunden-TTL. Alle Prüfungen bestanden. Docker Compose wurde hier nicht ausgeführt.

## Präsentation und Grenzen

Die acht Folien stehen in [docs/Praesentation.pdf](docs/Praesentation.pdf), die kurzen Vortragsnotizen in [docs/VORTRAG.md](docs/VORTRAG.md). Der Ablauf umfasst 15 Minuten einschließlich vier Minuten Live-Demo. Gruppennamen vor dem Vortrag ergänzen.

Der Prototyp verwendet einen einzelnen Broker und Auswertungen im Arbeitsspeicher. Mehrfachzustellung ist möglich, weil ausgehende Nachrichten und Quell-ACK keine gemeinsame Transaktion bilden. Monitoringwerte sind keine dauerhafte Buchhaltung. Bei einem Absturz können Temperaturwerte in einem neuen Fenster erneut erscheinen. Wiederholte echte Prozessabstürze durch einen unbekannten Programmfehler benötigen in einer produktiven Lösung einen dauerhaften Zustellungszähler. Die TTL begrenzt wartende Nachrichten und ist kein Zeitlimit für bereits ausgelieferte Verarbeitung.

Technische Details: [ACKs und Confirms](https://www.rabbitmq.com/docs/confirms), [DLX](https://www.rabbitmq.com/docs/dlx), [TTL](https://www.rabbitmq.com/docs/ttl), [Prioritäten](https://www.rabbitmq.com/docs/priority).
