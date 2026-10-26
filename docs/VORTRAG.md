# Vortragsnotizen: 15 Minuten mit vier Minuten Live-Demo

Acht Folien. Gruppennamen im PDF vor dem Vortrag ergänzen. Bei drei Personen übernimmt A Folien 1-2, B Folien 3-4 und C Folien 5-8. Bei vier bis sechs Personen die übrigen Folien und die Bedienung der Demo aufteilen. Alle Gruppenmitglieder übernehmen einen aktiven Teil.

## 1. IoT-Sensordaten mit RabbitMQ (0:30)

Drei simulierte Sensortypen liefern kontinuierlich Werte. Die Aufgabe verlangt gezieltes Routing, Auswertung, zuverlässige Verarbeitung und Überwachung. Python, Pika und RabbitMQ bilden dafür die gesamte Laufzeittechnik.

## 2. Architektur mit zwei Exchanges (2:00)

Ein Producer erzeugt Temperatur, Feuchtigkeit und Vibration. `iot.sensors` als Topic-Exchange routet Temperatur/Feuchtigkeit an die Umgebung und Vibration an die Maschine. Das Sternchen ersetzt genau einen Schlüsselbestandteil, hier die Sensor-ID.

`iot.events` als Direct-Exchange verwendet feste Schlüssel: WARNING/CRITICAL für die Alarmqueue, monitor für Monitoring und dead für die DLQ. Ein Exchange speichert nicht anstelle einer Queue. Mehrere Consumer derselben Queue teilen die Arbeit.

Alternative erläutern: Fanout würde alle Sensordaten an beide Analysen kopieren. Zwei Exchanges halten dieses kleine Projekt übersichtlich und erfüllen die geforderten unterschiedlichen Exchange-Typen.

Vorlesungsbezug: Kapitel 2, asynchrone und lose gekoppelte Kommunikation. Kapitel 3, PDF-Seiten 69-79, Topic und Direct.

## 3. JSON und fachliche Regeln (1:30)

Die Pflichtfelder sensor_id, timestamp, type und value stehen direkt im JSON. Der Routing Key muss zum Inhalt passen. Die Temperaturbeispiele 92,5 °C und 110 °C ergeben WARNING beziehungsweise CRITICAL.

Feuchtigkeit und Vibration besitzen ebenfalls je zwei Alarmgrenzen. Die Grenzwerte sind Annahmen für die Simulation. In einer echten Anlage wären sie fachlich zu kalibrieren. JSON ist lesbar und entspricht dem geforderten Format.

## 4. Persistenz, Confirms und Aggregation (2:00)

Ersten Codeausschnitt erklären: durable Queues, delivery_mode=2, Publisher Confirms und mandatory. Ein Confirm bestätigt die Brokerannahme. Ein Consumer-ACK bestätigt die Verarbeitung.

Der Producer versucht nach einem Fehler höchstens fünfmal erneut zu senden. Die Pausen steigen mit 1, 2, 4, 8 und 10 Sekunden. Ein verlorenes Confirm kann zu Mehrfachzustellung führen.

Fensterablauf erklären: Temperaturwerte zehn Sekunden nach Empfangszeit sammeln, Aggregation und Monitoring publizieren, danach die Lieferungen bestätigen. So bleiben die Quellmessungen bei einem früheren Absturz erneut zustellbar. Der Timer läuft auch ohne weitere Nachrichten.

Vorlesungsbezug: Kapitel 3, PDF-Seiten 116 und 125, Backoff und ACKs. Zusätzliche Präzisierung: https://www.rabbitmq.com/docs/confirms.

## 5. Alarme und Monitoring (1:30)

WARNING hat Priorität 3, CRITICAL Priorität 9. Die Alarmqueue konfiguriert x-max-priority=9. Prefetch 1 lässt den Broker wartende hohe Prioritäten bevorzugen. Eine laufende Verarbeitung wird nicht unterbrochen.

Monitoring erhält eigene Nachrichten über RabbitMQ und zählt Erfolge insgesamt, Erfolge je Typ, Alarme, fehlgeschlagene Verarbeitung und mittlere Verarbeitungszeit. Es liest keine Variablen aus den Analyseprozessen. Die Kennzahlen leben im Arbeitsspeicher und sind keine dauerhafte Statistik.

## 6. Fehlerbehandlung und TTL (1:00)

Ungültige Nachrichten bekommen NACK mit requeue=False. Den kurzen Codeausschnitt erklären. RabbitMQ führt die Originalnachricht über den Direct-Exchange und den Schlüssel dead in die DLQ. Es gibt keinen automatischen Rückweg.

Die Hauptqueues des Normalbetriebs haben standardmäßig 60.000 ms Nachrichten-TTL. Abgelaufene wartende Nachrichten gelangen mit x-death.reason=expired in die DLQ. Die DLQ selbst hat keine TTL. Diese TTL ist kein Verarbeitungszeitlimit für eine bereits ausgelieferte Nachricht. Die separaten Vorführungsqueues der folgenden Demos haben keine Nachrichten-TTL, damit Nachrichten während einer Erklärungspause erhalten bleiben.

## 7. Live-Demo (4:00)

Vorher Abhängigkeiten installieren und Broker starten. Zwei Terminals und die [RabbitMQ-Verwaltung](http://localhost:15672) bereithalten. Unter **Queues and Streams** nach `iot.demo.` filtern. Eine Demonstration vor dem Vortrag vollständig proben. Im interaktiven Terminal wartet die Demo an den entscheidenden Stellen auf **Enter**; erst nach der Aktualisierung der Queue-Zähler fortfahren.

1. `python start.py`: Werte, Alarm und Monitoring zeigen. Auf das Temperaturaggregat nach etwa zehn Sekunden warten. Etwa eine Minute einplanen.
2. `python demo.py invalid`: In `iot.demo.invalid.environment` zuerst **Ready** zeigen. Dann den Consumer starten lassen und den Wechsel nach `iot.demo.invalid.dlq` erklären: NACK ohne Requeue, `x-death.reason=rejected`. Die anschließende gültige Messung erzeugt ein Erfolgsereignis in `iot.demo.invalid.monitor`. Etwa eine Minute.
3. `python demo.py crash`: In `iot.demo.crash.environment` die Zustände **Ready**, **Unacked** und nach dem Prozessabbruch erneut **Ready** zeigen. Der Ersatzconsumer startet nach der nächsten Enter-Pause und bestätigt die Verarbeitung. In `iot.demo.crash.monitor` die aktuelle `crash-demo-…`-ID und `redelivered: true` im JSON-Body zeigen. Etwa eineinhalb Minuten.
4. Ergebnis einordnen und Normallauf mit Strg+C beenden. Etwa eine halbe Minute.

Nach dem Prozessabbruch kann **Unacked** sichtbar bleiben, bis der Broker die verlorene Verbindung erkannt hat. Bei verzögerter Erkennung bis etwa 90 Sekunden einplanen. Die Demo wartet dann selbst auf **Ready**; erst bei der nächsten Enter-Aufforderung fortfahren. Den Vier-Minuten-Ablauf mit dem tatsächlichen Broker proben und für diese Wartezeit gegebenenfalls zusätzliche Reserve vorsehen.

Die Demo-Queues und ihre DLQ-/Monitoringnachweise bleiben nach dem Lauf erhalten. Bereits bestätigte Quellmessungen sind aus der `.environment`-Queue entfernt. Zum Anzeigen der Nachweise unter **Get messages** immer **requeue** wählen. Während des Laufs keine Quellnachrichten über die Oberfläche abrufen oder entfernen. Das äußere Redelivery-Kennzeichen angezeigter Ereignisse kann sich durch dieses Lesen ändern; der Crash-Nachweis steht im JSON-Body des Monitoringereignisses.

Jeder Lauf druckt neue IDs mit einem eindeutigen Suffix aus. Bei einer zuvor abgebrochenen Vorführung erst ihren Consumer beenden und gegebenenfalls ausschließlich verbliebene Nachrichten ihrer `.environment`-Queue entfernen, bevor die Demo erneut startet. DLQ und Monitoring können die alten Nachweise behalten. `--auto` überspringt alle Enter-Pausen; ohne interaktives Terminal ist dieser Modus ebenfalls aktiv. `--step` schaltet Enter-Pausen auch bei umgeleiteter Eingabe ein. Automatisierte Tests verwenden getrennte Namensräume und räumen nur ihre eigenen Ressourcen auf.

Demoergebnisse beschreiben, die tatsächlich beobachtet wurden. Keine Installation oder neuen Testfälle während der vier Minuten einplanen. Die Enter-Pausen beim Probelauf so einteilen, dass genug Zeit für die Zustandsanzeige bleibt.

## 8. Fehlervergleich und Grenzen (2:30)

Die sieben Fragen aus der Aufgabe beantworten: Auslöser, Nachrichtenweg, Bestätigung, erneute Zustellung, Fehlerbehandlung, DLQ-Bedingung und anschließender Normalbetrieb.

Bei invalid löst die Validierung den Fehler aus. NACK ohne Requeue führt in die DLQ. Keine erneute Hauptqueue-Zustellung. Eine gültige Folgenachricht zeigt die Erholung. Beim Crash sendet der beendete Prozess weder ACK noch NACK. RabbitMQ reiht die unbestätigte Lieferung erneut ein. Der Ersatzconsumer bestätigt nach Erfolg. Der Crash allein erzeugt keinen Dead Letter.

Kapitel 3, PDF-Seite 121 / aufgedruckte Folie 123, fasst den Absturz als DLQ-Auslöser zu stark zusammen. Für unsere Classic-Queues ist zuerst Requeue korrekt. Siehe https://www.rabbitmq.com/docs/confirms und https://www.rabbitmq.com/docs/dlx.

Grenzen: einzelner Broker, RAM-basierte Auswertung, mögliche Duplikate zwischen Publish und ACK. Ein produktives System bräuchte bei Bedarf dauerhafte idempotente Verarbeitung und Brokerreplikation. Die kleine Lösung zeigt die Vorlesungskonzepte direkt in wenigen Python-Dateien.
