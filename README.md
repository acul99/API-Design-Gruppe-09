# IoT-Echtzeitanalyse mit RabbitMQ

Ein ausführbares Python-Projekt zur Bonusaufgabe „Echtzeit-Analyse von IoT-Sensordaten mit RabbitMQ“. Drei simulierte Sensortypen liefern JSON-Nachrichten. RabbitMQ verteilt sie an zwei Analyse-Consumer. Separate Consumer bearbeiten priorisierte Alarme und zeigen über RabbitMQ übertragene Monitoringdaten an.

## Schnellstart mit lokalem RabbitMQ

Voraussetzungen: Python ab 3.11 und ein laufender RabbitMQ-Broker. Die Anwendung benötigt als Laufzeitbibliothek ausschließlich Pika. Die Befehle im Projektordner ausführen.

Wenn `.venv` bereits eingerichtet ist, genügt direkt
`.\.venv\Scripts\python.exe -m iot_analysis run`. Für eine neue Installation:

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
.\.venv\Scripts\python.exe -m iot_analysis check
.\.venv\Scripts\python.exe -m iot_analysis run
```

Unter Linux/macOS entsprechend `python3 -m venv .venv` und `.venv/bin/python` verwenden. Ist Python bereits aktiviert, genügt in allen folgenden Beispielen `python`.

Falls der Windows-Python-Launcher `py` fehlt, ein installiertes `python` oder `uv venv .venv` verwenden. Eine bereits eingerichtete Projektumgebung lässt sich unmittelbar mit `.\.venv\Scripts\python.exe -m iot_analysis run` starten. Das Hilfsskript `powershell -ExecutionPolicy Bypass -File scripts\Start-Demo.ps1 -Mode run` verwendet ebenfalls diese Umgebung.

`run` startet Producer, beide Analyse-Consumer, Alarm-Consumer und Monitoring-Consumer als getrennte Prozesse. Nach spätestens zehn Sekunden erscheint eine Temperaturaggregation. `Strg+C` beendet die gestarteten Prozesse. Die RabbitMQ-Daten bleiben erhalten.

Die Standardverbindung lautet `amqp://guest:guest@localhost:5672/%2F`. RabbitMQ beschränkt das Konto `guest` standardmäßig auf lokale Verbindungen. Ein eigener Broker lässt sich über die Umgebung einstellen:

```powershell
$env:AMQP_URL = 'amqp://benutzer:passwort@localhost:5672/%2F'
$env:IOT_NAMESPACE = 'meine_gruppe'
python -m iot_analysis run
```

Bei Sonderzeichen im Benutzernamen, Passwort oder Virtual Host die URL-Bestandteile entsprechend URL-kodieren. Der Namespace trennt die Exchanges und Queues verschiedener Projektinstanzen im selben Virtual Host.

## Alternative mit Docker Compose

```powershell
docker compose up --build
```

Compose stellt Broker und Anwendung bereit. Für einen bereits lokal laufenden Broker den lokalen Schnellstart verwenden, damit kein zweiter Dienst dieselben Ports beansprucht. `docker compose down` beendet die Container. Ein gegebenenfalls verwendetes Broker-Volume nur löschen, wenn dessen Nachrichten ausdrücklich verworfen werden sollen.

Die Management-Oberfläche ist unter `http://localhost:15672` erreichbar. Compose
verwendet standardmäßig `iot` / `iot-demo-password` (über `.env` änderbar).
Die Anwendung selbst liest `AMQP_URL` und `IOT_NAMESPACE` aus der Prozessumgebung;
sie lädt eine `.env`-Datei nicht automatisch. Die `.env.example` dokumentiert beide Wege.
Eine isolierte Demo innerhalb von Compose startet mit
`docker compose --profile tools run --rm demo demo --scenario all`.

Die Compose-Demo lässt sich in einem zweiten Terminal mit `docker compose --profile tools run --rm demo demo --scenario crash` ausführen. Das erste `demo` bezeichnet dabei den Compose-Dienst, das zweite den Unterbefehl der Anwendung.

## Dienste einzeln starten

Für eine Live-Demo die Dienste in getrennten Terminals starten. So lassen sich Logausgaben und ein gezielter Consumer-Ausfall leichter beobachten.

```text
python -m iot_analysis topology
python -m iot_analysis monitor
python -m iot_analysis alarm
python -m iot_analysis analysis --kind environment
python -m iot_analysis analysis --kind machinery
python -m iot_analysis producer
```

Der Producer simuliert Temperatur, Luftfeuchtigkeit und Vibration. Mit `--count 30 --interval 0.1 --seed 42` entsteht ein begrenzter, reproduzierbarer Lauf. `--count 0` bedeutet kontinuierliche Erzeugung.

## Fehlerszenarien und Tests

```text
python -m iot_analysis demo --scenario invalid
python -m iot_analysis demo --scenario processing
python -m iot_analysis demo --scenario crash
python -m iot_analysis demo --scenario priority
python -m iot_analysis demo --scenario ttl
python -m iot_analysis demo --scenario all
python -m iot_analysis inspect --queue dlq
python -m unittest discover -s tests -v
```

Jedes Demo-Szenario verwendet eigene RabbitMQ-Ressourcen und räumt diese anschließend auf. Die regulären Queues werden dabei nicht geleert. `ttl` verwendet zusätzlich eine Nachrichten-TTL von einer Sekunde für einen schnellen Ablaufnachweis. `demo --scenario ttl --full-ttl` prüft die echte Queue-Grenze von 60 Sekunden ohne diese Verkürzung. Für die 15-minütige Präsentation reichen `invalid` und `crash` als zwei unterschiedliche Fehlerszenarien. `priority` ist eine kurze zusätzliche Demonstration.

Broker-Integrationstests explizit aktivieren:

```powershell
$env:RUN_RABBITMQ_TESTS = '1'
python -m unittest discover -s tests/integration -v
```

Mit zusätzlichem `$env:RUN_FULL_TTL_TEST = '1'` wird auch der ungefähr eine Minute
dauernde Test der echten Queue-TTL ausgeführt. Ohne diese Variable ist nur dieser
Langzeittest übersprungen.

Die dokumentierten, tatsächlich ausgeführten Prüfungen stehen in [docs/TESTBERICHT.md](docs/TESTBERICHT.md).

## Nachrichtenfluss

```mermaid
flowchart LR
    P[Producer: Temperatur / Feuchte / Vibration] --> S[topic: sensors]
    S -->|sensors.temperature.* und sensors.humidity.*| E[Analyse Umgebung]
    S -->|sensors.vibration.*| M[Analyse Maschine]
    E --> A[direct: alarms]
    M --> A
    A --> Q[Alarmqueue: Priorität 3 oder 9]
    Q --> AC[Alarm-Consumer]
    E --> T[fanout: telemetry]
    M --> T
    AC --> T
    T --> MC[Monitoring-Consumer]
    E -. NACK ohne Requeue / TTL .-> D[direct: dlx]
    M -. NACK ohne Requeue / TTL .-> D
    Q -. NACK ohne Requeue / TTL .-> D
    MC -. NACK ohne Requeue / TTL .-> D
    D --> DLQ[DLQ ohne TTL]
```

Die tatsächlichen Namen tragen das Präfix `iotlab.` oder den konfigurierten Namespace. Ein Exchange routet Nachrichten an gebundene Queues. Mehrere Consumer derselben Queue teilen sich deren Arbeit.

## Erfüllung der Aufgabenstellung

| Anforderung | Umsetzung |
| --- | --- |
| Mindestens drei Sensortypen | `temperature`, `humidity`, `vibration` |
| JSON-Pflichtfelder | `sensor_id`, `timestamp`, `type`, `value` direkt im JSON |
| Persistenz und Producer-Retry | Durable Topologie, `delivery_mode=2`, Publisher Confirms, maximal fünf Wiederholungen mit 1/2/4/8/10 Sekunden |
| Zwei Analyse-Consumer und Brokerfilterung | Eigene Queues mit Topic-Bindings für Umgebung und Maschine |
| Zehn-Sekunden-Aggregation | Temperaturfenster nach Empfangszeit mit Anzahl, Mittelwert, Minimum und Maximum |
| Anomalien und separate Alarme | Grenzwerte für alle drei Typen, WARNING und CRITICAL |
| Bevorzugte Alarmverarbeitung | Classic-Priority-Queue mit `x-max-priority=9`, Nachrichtenpriorität 3/9, Prefetch 1 |
| ACK/NACK und begrenzte Fehlerbehandlung | ACK erst nach Verarbeitung, dauerhafte Fehler mit `requeue=False` in die DLQ |
| Mindestens zwei Exchange-Typen | Topic, Direct und Fanout |
| TTL und DLQ | Hauptqueues 60.000 ms, DLX-Routing bei Ablehnung und Ablauf |
| Monitoring über RabbitMQ | Eigene Telemetrie-Nachrichten und separater Consumer |
| Reproduzierbarer Consumer-Ausfall | `demo --scenario crash` |
| Weiteres Fehlerszenario | `invalid`, zusätzlich `processing` |
| Präsentation | Zehn PDF-Folien, Vortragsnotizen und Demo-Ablauf |

## Materialien und Aufbau

- [Architektur, Verträge und Folienbezug](docs/ARCHITEKTUR.md)
- [Live-Demo mit erwarteter Beobachtung und Fehlererklärung](docs/DEMO.md)
- [Präsentation als PDF](docs/Praesentation.pdf)
- [Vortragsnotizen und Rollenverteilung](docs/VORTRAG.md)
- [Testbericht](docs/TESTBERICHT.md)

`src/iot_analysis/` enthält die Anwendung, `tests/` die Tests und `scripts/` die Start- und Präsentationsskripte. `python -m iot_analysis --help` und die Hilfe der jeweiligen Unterbefehle beschreiben die verfügbaren Optionen.

Die Lösung verwendet die in der Vorlesung behandelten RabbitMQ-Konzepte. gRPC ist für den asynchronen Nachrichtenfluss dieser Aufgabe nicht erforderlich. Ein einzelner Broker und RAM-basierte Auswertungen halten den Prototyp nachvollziehbar. Grenzen wie mögliche Mehrfachzustellung, fehlende Brokerreplikation und nicht transaktionale Nebeneffekte sind in der Architekturbeschreibung ausdrücklich erläutert.

Die PDF-Präsentation kann mit `python -m pip install reportlab` und `python scripts/build_presentation.py` erneut erzeugt werden. ReportLab ist nur für die Präsentation erforderlich.

`python scripts/package_project.py` erstellt `output/IoT-Realtime-Analysis.zip`
mit Quellcode, Tests, Dokumentation und Präsentation. Virtuelle Umgebung,
Zugangsdaten aus `.env` und temporäre Logs werden dabei ausgelassen.
