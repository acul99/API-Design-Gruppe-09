# IoT-Sensordaten mit RabbitMQ

Ein kleines Vorlesungsprojekt mit Python und Pika: Drei simulierte Sensortypen liefern Messwerte. Zwei Analyse-Consumer erkennen Anomalien, ein Alarm-Consumer verarbeitet priorisierte Alarme und ein Monitoring-Consumer zeigt Kennzahlen. Die Anwendung besteht aus sieben Python-Dateien.

Zur Vorbereitung auf die Präsentation: [Projekt und Technologien verstehen](docs/PROJEKT_VERSTEHEN.md). Der Leitfaden erklärt den Nachrichtenfluss, die RabbitMQ-Begriffe, Fehlerfälle und typische Rückfragen.

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
| `demo.py` | Zwei Fehlerszenarien schrittweise mit sichtbaren RabbitMQ-Queues vorführen |
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
- **Wiederverbindung:** Analyse, Alarm und Monitoring starten ihre Sitzung nach Verbindungsabbrüchen oder einer Abmeldung durch den Broker (`Basic.Cancel`) erneut. Sie öffnen Verbindung und Kanal neu, deklarieren Queues und Bindings und melden den Consumer wieder an. Das gilt auch, wenn eine Queue zwischen Deklaration und Anmeldung verschwindet (`404`) oder eine Alarm-/Monitoringpublikation wegen fehlender Bindings zurückkommt (`UnroutableError`). Die Quellnachricht bleibt bis zur erfolgreichen Verarbeitung unbestätigt. Die Pausen betragen 1, 2, 4, 8 und danach höchstens 10 Sekunden, ohne feste Versuchszahl. Nach einer mindestens 60 Sekunden stabilen Sitzung beginnt die Wartefolge wieder bei 1 Sekunde. Auch abgebrochene Verbindungsaufbauten werden wiederholt: Pika kann dabei einen nur vermuteten Authentifizierungs-, Zugriffs- oder Protokollfehler melden. Bei dauerhaft wiederkehrenden Meldungen `AMQP_URL`, Zugangsdaten und Broker prüfen. Eindeutig gemeldete Zugriffsverweigerungen, widersprüchliche Queue-Einstellungen und Programmfehler beenden den Dienst. Bei ausgeschöpften Producer-Retries beendet der Starter weiterhin das Gesamtsystem. Neue Consumer-Sitzungen beginnen mit leeren Temperaturfenstern bzw. Monitoringzählern. Eine gelöschte Queue wird neu angelegt; ihre gelöschten Nachrichten lassen sich dadurch nicht wiederherstellen.
- **Nachrichtenprüfung:** Vor der Verarbeitung wird auch die vollständige weiterzugebende JSON-Nachricht geprüft. Nicht endliche Zahlen in Zusatzfeldern und Nachrichten-IDs mit mehr als 255 UTF-8-Bytes werden ohne Requeue abgelehnt und gelangen in die DLQ.
- **Manuelle ACKs:** Erst nach erfolgreicher Verarbeitung und bestätigter Alarm-/Monitoringpublikation bestätigen. Temperaturwerte bleiben bis zum abgeschlossenen Zehn-Sekunden-Fenster unbestätigt.
- **Aggregation:** Alle innerhalb eines zusammenhängenden Zehn-Sekunden-Fensters empfangenen gültigen Temperaturen ergeben Anzahl, Mittelwert, Minimum und Maximum. Ein Timer schließt das Fenster auch ohne neue Nachricht ab.
- **Priorität:** Alarmqueue mit `x-max-priority=9`, WARNING mit Priorität 3, CRITICAL mit 9. Prefetch 1 begrenzt vorab ausgelieferte Alarme. Bereits laufende Verarbeitung wird nicht unterbrochen.
- **TTL und DLQ:** Hauptqueues des Normalbetriebs haben standardmäßig `x-message-ttl=60000` und leiten abgelaufene oder mit `requeue=False` abgelehnte Nachrichten über `iot.events` / `dead` an die DLQ. Die DLQ hat keine TTL und keine automatische Rückführung.
- **Monitoring:** Ein eigener Consumer zählt verarbeitete Messungen, Werte je Typ, Alarme, Fehler und mittlere Verarbeitungszeit anhand eigener RabbitMQ-Nachrichten.

Diese Begriffe stammen hauptsächlich aus Kapitel 3 der Vorlesung. Kapitel 2 erklärt die lose gekoppelte, asynchrone Kommunikation. JSON und Python/Pika/RabbitMQ entsprechen der Aufgabenstellung. gRPC ist für diesen Nachrichtenfluss nicht erforderlich.

## Zwei reproduzierbare Fehlerszenarien

```powershell
python demo.py invalid
python demo.py crash
```

Die Demos verwenden feste Queues unter `iot.demo.invalid.*` beziehungsweise `iot.demo.crash.*`. Sie bleiben nach dem Lauf in RabbitMQ erhalten, einschließlich der DLQ-Nachricht und der Monitoringereignisse. Bereits erfolgreich bestätigte Quellnachrichten sind dagegen aus ihrer Queue entfernt. Die normalen Anwendungsqueues unter `iot.*`, etwa `iot.environment`, werden von den Demos nicht verwendet.

Im interaktiven Terminal wartet die Demo an den entscheidenden Stellen auf **Enter**. Öffne parallel die [RabbitMQ-Verwaltung](http://localhost:15672), wähle **Queues and Streams** und filtere nach `iot.demo.`. Die wichtigsten Ansichten sind:

| Demo | Queue | Beobachtung |
| --- | --- | --- |
| `invalid` | `iot.demo.invalid.environment` | Die ungültige Nachricht wartet zunächst unter **Ready** auf ihren Consumer. |
| `invalid` | `iot.demo.invalid.dlq` | Nach der Ablehnung liegt die Nachricht hier mit `x-death.reason=rejected`. |
| `crash` | `iot.demo.crash.environment` | Die Nachricht wechselt von **Ready** zu **Unacked**. Nach dem Prozessabbruch wird sie wieder **Ready**, bevor der Ersatzconsumer sie verarbeitet. |
| beide | `iot.demo.invalid.monitor` bzw. `iot.demo.crash.monitor` | Die Verarbeitungsereignisse bleiben als Nachweis erhalten. |

Nach dem Prozessabbruch kann RabbitMQ zunächst weiter **Unacked** anzeigen, bis der Broker die verlorene Verbindung erkennt. Bei verzögerter Erkennung bis etwa 90 Sekunden einplanen. Die Demo wartet währenddessen selbst auf **Ready**; erst bei der nächsten Enter-Aufforderung fortfahren.

Warte nach einem Zustandswechsel auf die nächste Aktualisierung der Verwaltungsoberfläche, bevor du Enter drückst. Während des Laufs die Quellqueue nur anhand der Zähler beobachten. Nach dem Lauf kannst du in DLQ und Monitoringqueue unter **Get messages** die Option zum erneuten Einreihen (**requeue**) wählen. So bleibt die Nachricht auch nach dem Lesen erhalten. Beim Crash ist das Feld `redelivered: true` **im JSON-Body des Monitoringereignisses** der Nachweis für die erneute Zustellung der ursprünglichen Messung. Das äußere AMQP-Redelivery-Kennzeichen des gelesenen Ereignisses kann sich schon durch dessen wiederholte Anzeige ändern.

Jeder Lauf gibt neue IDs mit einem eindeutigen Suffix aus, zum Beispiel `crash-demo-…`. Dadurch sind die aktuellen Nachweise von älteren Läufen unterscheidbar. Die Demo löscht oder leert ihre Queues nicht automatisch. Falls eine abgebrochene Vorführung noch Quellnachrichten oder einen aktiven Consumer in ihrer `.environment`-Queue hinterlassen hat, verweigert sie den nächsten Start. Beende den alten Demo-Prozess und entferne vor einem neuen Lauf nur die verbliebenen Nachrichten aus genau dieser Demo-Queue; DLQ und Monitoringnachweise können erhalten bleiben.

**Für die Vorführung haben die Demo-Queues keine Nachrichten-TTL.** So läuft eine Nachricht während einer Erklärungspause nicht ab. Der Normalbetrieb verwendet weiterhin standardmäßig 60 Sekunden. `AMQP_URL` wählt auch für die Demos den Broker. `IOT_DEMO_NAMESPACE` kann den Basispräfix `iot.demo` ersetzen; der Szenarioname wird angehängt.

Für einen automatischen Durchlauf ohne Enter-Pausen:

```powershell
python demo.py invalid --auto
python demo.py crash --auto
```

Ohne interaktives Terminal laufen die Demos ebenfalls automatisch. Mit `--step` lassen sich Enter-Pausen auch bei umgeleiteter Eingabe ausdrücklich einschalten. In beiden Modi bleiben die Queues und Nachweise erhalten. Am Ende oder bei `Strg+C` werden nur die von der Demo gestarteten Consumer beendet.

| Szenario | Auslöser | ACK/NACK und Nachrichtenweg | Danach |
| --- | --- | --- | --- |
| `invalid` | Gezielt ungültige Sensornachricht | NACK ohne Requeue; tatsächlicher DLQ-Eintrag mit `x-death.reason=rejected` | Gültige Nachricht wird wieder verarbeitet |
| `crash` | Analyseprozess während der Verarbeitung beenden | Weder ACK noch NACK; Broker stellt unbestätigte Nachricht nach Neustart erneut zu | Ersatzconsumer verarbeitet und bestätigt sie |

Ein Consumer-Absturz allein führt bei Classic-Queues zunächst zur erneuten Zustellung. Die DLQ greift bei expliziter Ablehnung ohne Requeue oder beim Ablauf wartender Nachrichten. Diese Präzisierung ist für die Erklärung der Vorlesungsfolie zu DLQ-Auslösern wichtig. Ungültige Nachrichten werden sofort abgelehnt und durchlaufen keine endlose Requeue-Schleife.

Tests für die kleine Logik ausführen:

```powershell
python -m unittest discover -s tests -v
```

Mit `$env:RUN_RABBITMQ_TESTS = '1'` werden die beiden Fehlerdemos und die Wiederanmeldung aller vier Consumer nach dem Löschen ihrer eigenen Testqueue zusätzlich geprüft; dafür muss RabbitMQ unter `AMQP_URL` laufen. Die automatisierten Demotests setzen einen eigenen zufälligen Basispräfix, nutzen `--auto` und räumen ihre Testressourcen selbst auf. Die festen Vorführungsqueues bleiben davon unberührt.

Am 26.10.2026 geprüft: alle 38 Tests einschließlich der beiden Fehlerdemos und der vier Wiederanlaufprüfungen bestanden gegen einen separaten RabbitMQ 4.3.6 in Docker. Die Demotests prüfen auch wiederholte Läufe mit älteren Belegen, erhaltene Nachrichten und unveränderte Daten im normalen Namensraum. Die Tests ohne Broker prüfen außerdem Pikas echte Fehlerklassifikation bei abgebrochenen Handshakes und das Ende des Consumer-Generators bei `Basic.Cancel`. Zusätzlich lief das Gesamtsystem nach einem echten Broker-Stopp/Start weiter, einschließlich Aggregation und Monitoring. Das Löschen der Alarm- und Monitoringqueue bei laufenden Analysen löste ebenfalls einen erfolgreichen Wiederanlauf aus, einschließlich erneuter Publikation und ACK der Quellnachrichten.

## Präsentation und Grenzen

Die zehn deutschsprachigen Folien mit Vortragsnotizen stehen in [Gruppe-09-IoT-Praesentation.pptx](Gruppe-09-IoT-Praesentation.pptx). Der Ablauf umfasst 15 Minuten einschließlich vier Minuten Live-Demo. Gruppennamen vor dem Vortrag ergänzen.

Der Prototyp verwendet einen einzelnen Broker und Auswertungen im Arbeitsspeicher. Mehrfachzustellung ist möglich, weil ausgehende Nachrichten und Quell-ACK keine gemeinsame Transaktion bilden. Monitoringwerte sind keine dauerhafte Buchhaltung. Bei einem Absturz können Temperaturwerte in einem neuen Fenster erneut erscheinen. Wiederholte echte Prozessabstürze durch einen unbekannten Programmfehler benötigen in einer produktiven Lösung einen dauerhaften Zustellungszähler. Die TTL begrenzt wartende Nachrichten und ist kein Zeitlimit für bereits ausgelieferte Verarbeitung.

Technische Details: [ACKs und Confirms](https://www.rabbitmq.com/docs/confirms), [DLX](https://www.rabbitmq.com/docs/dlx), [TTL](https://www.rabbitmq.com/docs/ttl), [Prioritäten](https://www.rabbitmq.com/docs/priority).
