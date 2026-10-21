# Vortragsnotizen für 15 Minuten

Die PDF enthält zehn Folien. Die unten genannten Zeiten ergeben insgesamt 15 Minuten einschließlich vier Minuten Live-Demo. Namen vor der Präsentation ergänzen. Alle Gruppenmitglieder übernehmen einen aktiven Teil. Die Verteilung auf drei Rollen lässt sich bei vier bis sechs Personen aufteilen.

| Rolle | Name ergänzen | Inhalt |
| --- | --- | --- |
| A | ____________________ | Einstieg, Architektur, Routing |
| B | ____________________ | Nachrichtenvertrag, Zuverlässigkeit, Aggregation |
| C | ____________________ | Monitoring, Live-Demo, Grenzen |

Bei sechs Personen jeweils eine der Foliengruppen 1-2, 3-4, 5, 6, 7 und 8-10 übernehmen. Die Person an der Tastatur und die erklärende Person können wechseln. Änderungen an der Gruppengröße in der tatsächlichen Prüfungsorganisation mit der Lehrperson klären.

## Folie 1: IoT-Echtzeitanalyse mit RabbitMQ (0:30, Rolle A)

Unser Szenario ist die Überwachung einer simulierten Industrieanlage. Drei Sensortypen liefern fortlaufend Messwerte. Die Aufgabe ist eine robuste Nachrichtenarchitektur mit fachlichen Analysen und priorisierten Alarmen. Die verwendeten Technologien sind Python, Pika und RabbitMQ über AMQP.

## Folie 2: Architektur und Nachrichtenfluss (1:30, Rolle A)

Den Weg einer Temperaturnachricht erklären: Producer, Sensor-Exchange, Umgebungsqueue, Analyse. Bei einer Anomalie entsteht eine neue Alarmnachricht. Der Alarmconsumer bekommt sie über eine eigene Queue. Parallel publiziert die Analyse Telemetrie für den Monitoringconsumer. Die DLQ sammelt abgelehnte und abgelaufene Nachrichten.

Exchanges vermitteln zu Queues. Sie speichern die Sensordaten nicht anstelle der Queues. Mehrere Consumer derselben Queue würden Arbeit teilen. Separate gebundene Queues würden jeweils eine Kopie erhalten.

## Folie 3: Routing und Exchange-Wahl (1:15, Rolle A)

Topic passt zu `sensors.temperature.temp-01`, weil das Binding `sensors.temperature.*` den Sensortyp unabhängig von der ID auswählt. Die beiden Fachanalysen bekommen bereits im Broker unterschiedliche Messwerte. Ein Fanout für Sensordaten würde dafür unnötige Kopien erzeugen. Direct genügt für exakt benannte Alarmstufen. Fanout verteilt Monitoringereignisse an interessierte Auswertungen.

Quelle: Kapitel 3, PDF-Seiten 69-79, aufgedruckte Folien 70-80. Die Aussage „Direct ist 1:1“ präzise als exakter Key-Abgleich erklären. Mehrere Queues dürfen denselben Binding Key besitzen.

## Folie 4: JSON-Vertrag und Alarmgrenzen (1:00, Rolle B)

Die Pflichtattribute stehen direkt im JSON. Die AMQP-Eigenschaften tragen unter anderem Persistenz, Nachrichten-ID und Priorität. Typ und Routing Key müssen zueinander passen. Ungültige Daten gelangen in die DLQ. Grenzwerte sind für die Demonstration gewählt und müssten für eine echte Anlage fachlich festgelegt werden.

Ein Temperaturwert von 92,5 °C erzeugt WARNING. Ein Wert über 100 °C erzeugt CRITICAL. Auch Feuchtigkeit und Vibration besitzen zwei Alarmstufen. Die Zahl der Stufen erfüllt die Aufgabe ohne zusätzliche fachliche Komplexität.

## Folie 5: Senden und Bestätigen (1:30, Rolle B)

Codeausschnitt für `delivery_mode=2`, `mandatory=True` und Publisher Confirms erklären. Durable Queues und persistente Nachrichten ergänzen einander. Ein Publisher Confirm bestätigt die Brokerannahme. Der Consumer-ACK bestätigt die Verarbeitung.

Der Producer macht nach einem initialen Fehler höchstens fünf Wiederholungen. Die Wartezeiten steigen mit 1, 2, 4, 8 und 10 Sekunden. Ein verlorenes Confirm kann trotz erfolgreicher Brokerannahme einen erneuten Publish auslösen. Diese Unsicherheit macht Mehrfachzustellung möglich.

Quelle: Kapitel 3, PDF-Seiten 116 und 125, aufgedruckte Folien 118 und 127. Publisher-Confirm-Semantik zusätzlich offizielle RabbitMQ-Dokumentation: https://www.rabbitmq.com/docs/confirms.

## Folie 6: Zehn-Sekunden-Fenster und Fehlerpfad (1:30, Rolle B)

Die zehn Sekunden beziehen sich auf die im Consumer empfangenen gültigen Temperaturen. Eine monotone Uhr verhindert Effekte durch eine verstellte Systemzeit. Der Fensterabschluss läuft auch ohne neu eintreffende Nachricht. Ergebnis sind Anzahl, Mittelwert, Minimum und Maximum.

Temperaturlieferungen bleiben bis zum publizierten Aggregationsergebnis unbestätigt. Beim Crash kann der Broker die Messwerte erneut zustellen. Das verbessert die Wiederanlaufsemantik, garantiert aber keine identischen Fenster nach einem Ausfall und keine genau einmal erzeugten Alarme.

Dauerhafte Datenfehler enden mit NACK ohne Requeue. Verarbeitungsfehler haben höchstens drei lokale Versuche. Nach dem Limit führt der DLX in die DLQ. Verbindungsausfälle sind ein eigener Fall, weil ein ACK/NACK möglicherweise nicht mehr sendbar ist. Der zweite Codeausschnitt stammt gekürzt aus `flush_window()` und `_reject()` in `consumers.py`. Die Reihenfolge von Publish und ACK sowie `requeue=False` erklären.

Quelle: Kapitel 3, PDF-Seiten 119, 125 und 130, aufgedruckte Folien 121, 127 und 132.

## Folie 7: Priorität, TTL und Monitoring (1:30, Rolle C)

Die Classic-Priority-Queue nutzt `x-max-priority=9`, WARNING 3 und CRITICAL 9. Prefetch 1 reduziert bereits ausgelieferte, noch nicht bearbeitete Alarme. Es gibt keine Unterbrechung eines laufenden Handlers.

TTL 60.000 ms entfernt veraltete wartende Daten aus dem normalen Datenfluss. Abgelaufene Nachrichten gelangen als `expired` in die DLQ. Monitoring empfängt Nachrichten über RabbitMQ und berichtet die geforderten fünf Kennzahlen. Der Stand ist flüchtig und keine dauerhafte exakt einmal geführte Statistik.

Technische Quellen: https://www.rabbitmq.com/docs/priority, https://www.rabbitmq.com/docs/ttl und https://www.rabbitmq.com/docs/dlx.

## Folie 8: Live-Demo (4:00, Rolle C mit Unterstützung)

Den vorbereiteten Ablauf in `DEMO.md` verwenden. Normalbetrieb, Aggregation, ungültige Nachricht und Consumer-Crash sind Pflicht. Bei verbleibender Zeit Alarmpriorität zeigen. Das System nicht während des Vortrags erstmalig installieren. Bei einem unerwarteten lokalen Problem die konkrete Beobachtung ehrlich erklären und die zuvor ausgeführten Tests heranziehen. Keine Live-Ergebnisse behaupten, die nicht beobachtet wurden.

## Folie 9: Erklärung der Fehlerszenarien (1:15, Rolle C)

Die unterschiedlichen Pfade nebeneinander erklären: Ungültiges JSON führt zu NACK ohne Requeue und DLQ. Ein Prozessende vor ACK führt zur erneuten Zustellung. Ein dauerhafter Verarbeitungsfehler endet nach drei Versuchen mit NACK und DLQ. Die neue gültige Nachricht zeigt, dass das System anschließend weiterarbeitet.

Kapitel 3, PDF-Seite 121 / aufgedruckte Folie 123, fasst den Crash als DLQ-Auslöser zu stark zusammen. Für unsere Classic-Queue ist die erste Reaktion Requeue der unbestätigten Lieferung. Die offizielle Dokumentation präzisiert dies: https://www.rabbitmq.com/docs/confirms.

## Folie 10: Erkenntnisse und Grenzen (1:00, Rollen A/B/C)

Die zentrale Erkenntnis ist die Trennung zwischen Brokerannahme, Verarbeitung und fachlichen Nebeneffekten. Broker-Routing spart unnötige Verbraucherlogik. Begrenzte Versuche und die DLQ verhindern, dass bekannte fehlerhafte Nachrichten den Datenstrom blockieren.

Ein einzelner Broker bleibt ein Ausfallpunkt. Alarme, Telemetrie und ACK sind nicht atomar. Ein produktives System bräuchte dauerhafte idempotente Verarbeitung und gegebenenfalls Brokerreplikation. Die lokale Lösung bleibt dadurch klein genug, um die in der Vorlesung behandelten Mechanismen im Code und in der Live-Demo konkret zu erklären.
