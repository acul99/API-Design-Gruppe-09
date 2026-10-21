# Architektur und Entwurfsentscheidungen

## Ziel und fachliche Aufteilung

Der Prototyp entkoppelt die Erzeugung von Messwerten von deren Auswertung. Die Producer kennen den Sensor-Exchange und den Nachrichtenvertrag, aber keine Adressen von Analysediensten. RabbitMQ puffert und verteilt Nachrichten.

Der Umgebungskonsument verarbeitet Temperatur und Luftfeuchtigkeit. Der Maschinenkonsument verarbeitet Vibrationen. Der Alarmkonsument behandelt die von den Analysen erzeugten Alarme. Der Monitoringkonsument erhält eigene Verarbeitungsereignisse. Jeder Dienst kann als separater Python-Prozess laufen und besitzt seine eigene Verbindung.

## Exchanges und Queues

Die folgende Tabelle verwendet den Standardnamespace `iotlab`.

| Exchange | Typ | Zielqueue | Bindings |
| --- | --- | --- | --- |
| `iotlab.sensors` | topic | `iotlab.analysis.environment` | `sensors.temperature.*`, `sensors.humidity.*` |
| `iotlab.sensors` | topic | `iotlab.analysis.machinery` | `sensors.vibration.*` |
| `iotlab.alarms` | direct | `iotlab.alarm` | `WARNING`, `CRITICAL` |
| `iotlab.telemetry` | fanout | `iotlab.monitoring` | alle Telemetrieereignisse |
| `iotlab.dlx` | direct | `iotlab.dlq` | `dead` |

Alle Exchanges und Queues sind durable. Die Hauptqueues besitzen `x-message-ttl=60000`, `x-dead-letter-exchange=iotlab.dlx` und `x-dead-letter-routing-key=dead`. Die DLQ hat weder eine TTL noch eine Rückbindung in die Hauptqueues. Damit existiert kein automatischer DLQ-Zyklus.

Die Sensor-ID belegt genau ein Wort des Routing Keys. IDs enthalten deshalb keine Punkte. Ein Beispiel lautet `sensors.temperature.temp-01`. Eine zusätzliche Validierung im Consumer prüft die Konsistenz zwischen Routing Key und Inhalt. Sie ersetzt nicht das Routing im Broker.

## Warum diese Exchange-Typen?

**Topic für Sensoren:** Die Fachkategorie ist Bestandteil des Routing Keys. Wildcard-Bindings wählen einen Sensortyp unabhängig von der konkreten Sensor-ID aus. Neue Sensoren derselben Kategorie benötigen keine Änderung der Bindings. Ein Direct-Exchange wäre bei einem Key nur pro Sensortyp ebenfalls möglich, verliert aber die Möglichkeit, später nach Sensor oder weiteren Schlüsselbestandteilen zu routen. Fanout würde alle Sensordaten in jede Analysequeue kopieren und die gewünschte Filterung wieder in die Anwendung verlagern.

**Direct für Alarme:** Zwei exakt benannte Alarmstufen reichen aus. Beide Bindings zeigen bewusst auf dieselbe Priority-Queue. Die eigentliche Bevorzugung entsteht durch die AMQP-Priorität und die Queue-Konfiguration, nicht durch den Text `CRITICAL`. Zwei getrennte Queues würden eine zusätzliche faire oder gewichtete Abrufstrategie benötigen.

**Fanout für Telemetrie:** Alle Verarbeitungsereignisse gehen an den Monitoringdienst. Später können weitere Auswertungen eine eigene Queue anbinden. Mehrere Consumer derselben Queue bekommen dagegen keine vollständigen Kopien, sondern teilen die Arbeit. Eine Topic-Telemetrie wäre sinnvoll, sobald verschiedene Abonnenten nach Ereigniskategorien filtern sollen.

**Direct für Dead Letters:** Ein fester Routing Key führt sämtliche fehlgeschlagenen und abgelaufenen Nachrichten in eine gemeinsame, nachvollziehbare DLQ. Für eine größere Anwendung wären getrennte Fehlerqueues pro Fachbereich denkbar.

## Nachrichtenvertrag

Eine Sensornachricht enthält die vier geforderten Felder direkt im JSON. Weitere Metadaten dürfen hinzukommen. Der Zeitstempel verwendet ISO 8601 mit Zeitzone, der Wert eine endliche JSON-Zahl.

```json
{
  "sensor_id": "temp-01",
  "timestamp": "2026-10-21T10:00:00+00:00",
  "type": "temperature",
  "value": 92.5,
  "message_id": "eindeutige-id"
}
```

`temperature` wird in °C, `humidity` in Prozent relativer Luftfeuchtigkeit und `vibration` in mm/s simuliert. Die Anomaliegrenzen sind Annahmen für die Demonstration und müssen bei realen Anlagen fachlich kalibriert werden.

| Typ | WARNING | CRITICAL |
| --- | --- | --- |
| Temperatur | größer 80 °C | größer 100 °C |
| Luftfeuchtigkeit | kleiner 30 % oder größer 70 % | kleiner 15 % oder größer 85 % |
| Vibration | größer 5 mm/s | größer 10 mm/s |

CRITICAL hat Vorrang vor WARNING. Gleichheit mit einer Grenze erfüllt eine strikte „größer/kleiner“-Bedingung noch nicht.

Ein Alarm ist eine neue Nachricht mit Sensorbezug, Messwert, Stufe und stabiler Alarm-ID. WARNING erhält AMQP-Priorität 3, CRITICAL Priorität 9. Ein Telemetrieereignis enthält eine Ereignis-ID, den Dienst und den Ereignistyp sowie die fachlich notwendigen Kennzahlen. Die Anwendung verwendet JSON als lesbares, sprachunabhängiges Format.

## Zuverlässiges Senden

Persistente Nachrichten (`delivery_mode=2`) und durable Queues ergänzen sich. Publisher Confirms melden die Annahme durch den Broker. Mit `mandatory=True` werden nicht routbare Veröffentlichungen als Fehler sichtbar. Ein Confirm bestätigt nicht die erfolgreiche Verarbeitung durch einen Consumer.

Bei vorübergehenden Verbindungsfehlern folgen nach dem ersten Versuch höchstens fünf Wiederholungsversuche. Die Pausen betragen 1, 2, 4, 8 und 10 Sekunden. Ein noch nicht sicher bestätigter Publish kann erneut gesendet werden. Deshalb müssen Empfänger mit Duplikaten rechnen. Nach Ausschöpfen des Limits beendet sich der betroffene Dienst sichtbar mit Fehler. Es gibt keine versteckte unbegrenzte Producer-Schleife.

Beim Producer gilt dieses Limit pro zu sendender Messung. Bei Consumern hat jeder
Verbindungsaufbau denselben Backoff. Nach einem Abbruch einer bereits aufgebauten
Verarbeitungssession sind außerdem höchstens fünf Sessionneustarts möglich. Ein
ausgeschöpfter Verbindungsaufbau beendet den Dienst unmittelbar; sein Limit wird
nicht durch die Sessionneustarts vervielfacht.

## Verarbeitung, Aggregation und ACK

Die Analysen verwenden manuelle Bestätigungen. Ein normaler Luftfeuchte- oder Vibrationswert erhält sein ACK erst nach der fachlichen Verarbeitung und dem bestätigten Publish der erforderlichen Telemetrie. Bei einer Anomalie muss zuvor auch der Alarm-Publish erfolgreich bestätigt sein.

Die Temperaturaggregation benutzt zusammenhängende Zeitfenster von zehn Sekunden nach lokaler, monoton gemessener Empfangszeit. Der Producer-Zeitstempel dient der Nachvollziehbarkeit und verschiebt das Fenster nicht. Der Timer läuft auch, wenn keine neue Nachricht eintrifft. Ein nicht leeres Fenster ergibt Anzahl, arithmetischen Mittelwert, Minimum und Maximum.

Temperaturlieferungen bleiben bis zum erfolgreichen Abschluss ihres Fensters unbestätigt. Erst nach Ausgabe und bestätigtem Publish des Aggregationsergebnisses können ihre ACKs folgen. Dadurch kann ein Absturz die Messwerte erneut zustellen, statt bereits bestätigte Werte allein im verlorenen Arbeitsspeicher zurückzulassen. Der höhere Prefetch des Umgebungskonsumenten erlaubt währenddessen weitere Messwerte. Bei hoher Last begrenzt der Prefetch die Zahl gleichzeitig unbestätigter Lieferungen.

Eine erneute Zustellung nach einem Ausfall kann in ein neues Zeitfenster fallen. Ein im Broker bereits bestätigter Alarm oder ein publiziertes Aggregat kann trotz ausstehendem Sensor-ACK schon existieren. Der Prototyp garantiert deshalb keine globale Exactly-once-Auswertung und keine transaktionale Verbindung zwischen Alarm, Telemetrie und Quellnachricht.

## Fehlerbehandlung und DLQ

Syntaktisch oder semantisch ungültige Sensornachrichten können durch erneutes Verarbeiten nicht korrekt werden. Der Consumer sendet `basic_nack(..., requeue=False)`. RabbitMQ leitet die Nachricht über den konfigurierten DLX in die DLQ. Das `x-death`-Headerfeld zeigt unter anderem Ursache und Ursprungsqueue.

Für simulierte Verarbeitungsfehler stehen höchstens drei lokale Verarbeitungsversuche zur Verfügung. Ein transienter Demofehler gelingt beim nächsten Versuch. Ein dauerhafter Fehler endet nach dem Limit ebenfalls mit NACK ohne Requeue. Der Consumer schiebt die Nachricht nicht ständig unverändert in die Hauptqueue zurück. Die Failure-Telemetrie beschreibt die gescheiterte Verarbeitung.

Verbindungsfehler während der Verarbeitung sind anders zu behandeln: Ein ACK oder NACK ist dann möglicherweise nicht mehr übertragbar. Durch Schließen der Verbindung bleiben nicht bestätigte Lieferungen für eine erneute Zustellung verfügbar. Wiederholte echte Prozessabstürze durch einen unbekannten Programmfehler sind durch den lokalen Versuchszähler nicht prozessübergreifend begrenzt. Eine produktive Lösung bräuchte dafür einen persistenten Versuchszähler oder die passend konfigurierte Delivery-Limit-Funktion eines geeigneten Queuetyps.

### Präzisierung einer Vorlesungsfolie

Kapitel 3 nennt auf PDF-Seite 121, aufgedruckte Folie 123, auch einen beendeten Consumer als direkten DLQ-Auslöser. Für die hier verwendeten Classic-Queues gilt präziser: Bei Verlust des Channels oder der Connection werden unbestätigte Nachrichten normalerweise erneut eingereiht. Der Absturz allein leitet sie nicht in die DLQ. Unser Crash-Szenario zeigt diese erneute Zustellung mit `redelivered=True`. Erst eine spätere Ablehnung ohne Requeue oder ein anderer konfigurierter Dead-Letter-Auslöser führt in die DLQ.

## TTL und Priorität

Nachrichten in den Hauptqueues besitzen eine Queue-TTL von 60 Sekunden. Veraltete Messwerte sollen keinen beliebig alten Rückstau in eine aktuelle Auswertung tragen. RabbitMQ stellt abgelaufene Nachrichten nicht mehr als normale Ready-Nachrichten zu und dead-lettert sie mit Ursache `expired`. Die physische Entfernung kann von Queue-Zustand und Ablaufprüfung abhängen. Die DLQ bewahrt sie für die Untersuchung auf.

TTL ist kein Timeout für einen bereits beim Consumer befindlichen, unbestätigten Verarbeitungsvorgang. Sie ersetzt daher weder Prefetch noch ein angemessenes Verarbeitungszeitlimit.

Die Alarmqueue ist explizit eine Classic-Queue mit `x-max-priority=9`. Nur wenige Stufen halten den Verwaltungsaufwand gering. Der Alarmconsumer setzt Prefetch 1. So kann RabbitMQ bei wartenden Alarmen hohe Prioritäten bevorzugen. Ein bereits ausgelieferter oder in Verarbeitung befindlicher WARNING-Alarm wird durch einen späteren CRITICAL-Alarm nicht unterbrochen. Der Prioritätstest erzeugt deshalb zunächst einen Rückstau und startet danach die Verarbeitung.

## Monitoring über Nachrichten

Die Analysen veröffentlichen `sensor_processed`, `processing_failed` und Aggregationsereignisse über den Telemetrie-Exchange. Der Monitoringconsumer konsumiert ausschließlich seine RabbitMQ-Queue. Er zählt erfolgreiche Sensormessungen insgesamt und pro Typ, erzeugte Alarme sowie fehlgeschlagene Verarbeitungen und berichtet die mittlere Verarbeitungszeit.

Die Kennzahlen sind Prozessauswertungen der empfangenen Telemetrie und kein Zugriff auf den Arbeitsspeicher der Analysedienste. Ereignis-IDs erlauben begrenzte Deduplizierung. Nach Neustart, Ablauf von Telemetrie oder Überschreiten des Deduplizierungspuffers sind die Kennzahlen keine dauerhaft exakte Buchhaltung. Brokerseitig abgelaufene Messwerte erzeugen außerdem keine erfolgreiche Verarbeitungstelemetrie. Ihr Zustand lässt sich an DLQ und `x-death` erkennen.

## Bezug zu den Vorlesungsunterlagen

Die Seitenangaben beziehen sich auf die bereitgestellten PDFs. In Kapitel 3 weichen die aufgedruckten Foliennummern teilweise ab.

| Thema im Projekt | Vorlesungsbezug |
| --- | --- |
| Stabiler JSON-Vertrag, bekannte Metadaten | Kapitel 1, PDF-Seite 5. Kapitel 2, PDF-Seiten 6 und 19 |
| Vermittelte, asynchrone und lose gekoppelte Kommunikation | Kapitel 2, PDF-Seiten 22-29 und 47 |
| Broker als Puffer und Entkopplung | Kapitel 3, PDF-Seiten 5-8 |
| Pika und Routing mit Exchanges | Kapitel 3, PDF-Seiten 69-79, aufgedruckte Folien 70-80 |
| Begrenzte Retries und exponentieller Backoff | Kapitel 3, PDF-Seite 116, aufgedruckte Folie 118 |
| DLX und DLQ | Kapitel 3, PDF-Seiten 117-121, aufgedruckte Folien 119-123 |
| Manuelle ACKs/NACKs | Kapitel 3, PDF-Seite 125, aufgedruckte Folie 127 |
| Prefetch, Poison Messages und Monitoring | Kapitel 3, PDF-Seiten 130-131, aufgedruckte Folien 132-133 |
| Abgrenzung zu direkter Kommunikation mit sofortigem Ergebnis | Kapitel 5, PDF-Seiten 5-7 |

gRPC oder Protobuf würden für diese Aufgabe zusätzliche Kommunikationswege bzw. ein anderes Datenformat einführen. Die Aufgabenstellung verlangt JSON und RabbitMQ/Pika. Der Prototyp konzentriert sich daher auf diese Konzepte.

## Grenzen und mögliche Erweiterungen

- Ein einzelner Broker und nicht replizierte Classic-Queues bieten keine Hochverfügbarkeit bei Rechner- oder Datenträgerausfall.
- RAM-basierte Aggregation und Monitoring sind bewusst einfach. Eine dauerhafte Datenbank und idempotente Verarbeitung würden die Wiederanlaufsemantik verbessern.
- Publishes und Quell-ACK bilden keine gemeinsame Transaktion. Für strengere Anforderungen wären Inbox-/Outbox-Muster oder eine passende Transaktionsstrategie zu untersuchen.
- Bei mehr Last müssten Empfangsraten, Prefetch und Aggregationspartitionen gemeinsam entworfen werden. Mehrere gleichartige Analyseinstanzen würden ihre Queue teilen und getrennte lokale Fenster bilden.
- Die simulierten Grenzwerte und Lasten stellen keine produktive Anlagenüberwachung dar.

## Technische Quellen

Die Implementierung folgt den bereitgestellten Vorlesungsunterlagen und präzisiert das Verhalten anhand der offiziellen Dokumentation:

- [RabbitMQ: Consumer Acknowledgements and Publisher Confirms](https://www.rabbitmq.com/docs/confirms)
- [RabbitMQ: Dead Letter Exchanges](https://www.rabbitmq.com/docs/dlx)
- [RabbitMQ: Time-to-Live and Expiration](https://www.rabbitmq.com/docs/ttl)
- [RabbitMQ: Priority Support in Queues](https://www.rabbitmq.com/docs/priority)
- [Pika: Blocking Connection Adapter](https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html)
