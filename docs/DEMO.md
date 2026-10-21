# Live-Demo und reproduzierbare Fehlerszenarien

## Vorbereitung vor dem Vortrag

1. Python-Umgebung aktivieren und `python -m iot_analysis check` ausführen.
2. Unit- und Integrationstests ausführen. Das Datum und Ergebnis im Testbericht aktuell halten.
3. Broker und Abhängigkeiten vor dem Termin installieren. Während der Präsentation keine Pakete herunterladen.
4. Ein Terminal für `python -m iot_analysis run` und ein zweites für die Fehlerszenarien bereithalten.
5. Die Befehle einmal vorab ausführen. Namen und Sprecherrollen in den Vortragsnotizen ergänzen.

Die Szenarien erstellen eigene Namespaces. Sie beeinflussen deshalb den gleichzeitig laufenden normalen Datenstrom nicht. Nach Abschluss löschen sie ausschließlich ihre eigenen Demo-Ressourcen. Für eine dauerhafte DLQ-Ansicht die im normalen Namespace entstehenden Dead Letters mit `python -m iot_analysis inspect --queue dlq` ansehen.

## Ablauf für vier Minuten Live-Demo

| Zeit | Aktion | Erläuterung |
| --- | --- | --- |
| 0:00-0:40 | `python -m iot_analysis run` | Drei Typen, zwei Fachanalysen, separate Alarme und Monitoring zeigen |
| 0:40-1:00 | Eine `aggregation`-Ausgabe markieren | Temperatur, zehn Sekunden, Mittelwert und Anzahl erklären |
| 1:00-1:40 | `python -m iot_analysis demo --scenario invalid` | Ungültige Nachricht und tatsächlichen DLQ-Eintrag zeigen |
| 1:40-2:50 | `python -m iot_analysis demo --scenario crash` | Consumer endet vor ACK. Neustart verarbeitet dieselbe Lieferung erneut |
| 2:50-3:25 | `python -m iot_analysis demo --scenario priority` | CRITICAL passiert wartende WARNING-Alarme |
| 3:25-4:00 | Monitoring und Fazit | Erfolgs-/Fehlerpfad erläutern. Normallauf mit `Strg+C` beenden |

Die Zeitangaben sind ein geplanter Ablauf. Laufzeiten hängen von Broker und Rechner ab. Der echte Queue-TTL-Test mit `--full-ttl` braucht mindestens etwa eine Minute und passt besser in die Vorbereitung als in die Live-Demo. Der kurze TTL-Test nutzt eine zusätzliche Nachrichten-TTL von einer Sekunde.

## Szenario 1: Ungültige Sensornachricht

```text
python -m iot_analysis demo --scenario invalid
```

1. **Auslöser:** Das Szenario veröffentlicht gezielt eine Nachricht, die den Sensorvertrag verletzt.
2. **Nachricht:** RabbitMQ routet sie anhand des gültigen Routing Keys in die passende Analysequeue. Die Inhaltsvalidierung schlägt fehl.
3. **ACK/NACK:** Der Consumer sendet NACK mit `requeue=False`.
4. **Erneute Zustellung:** Eine unveränderte Wiederholung in der Hauptqueue findet nicht statt.
5. **Fehlerbehandlung:** Ungültige Daten gelten als dauerhaft fehlerhaft. Ein weiterer Versuch würde sie nicht reparieren.
6. **DLQ:** Der konfigurierte DLX führt die Originalnachricht in die DLQ. `x-death.reason` zeigt `rejected`.
7. **Erholung:** Eine anschließende gültige Nachricht wird wieder normal verarbeitet. Der Fehler blockiert die Hauptqueue nicht.

**Beleg in der Demonstration:** `dlq_observed` zeigt die tatsächlich aus der DLQ gelesenen Originalnachrichten und ihre `x-death`-Ursache. Das Szenario prüft ungültiges JSON und einen fehlenden Messwert. `demo_passed` erscheint erst nach den Prüfungen und der erfolgreichen nachfolgenden Verarbeitung; `monitoring_failures=2` stammt aus der separaten Monitoringqueue. Die vollständigen Worker-Logs stehen im ausgegebenen Ordner unter `.state/`.

## Szenario 2: Consumer-Ausfall während der Verarbeitung

```text
python -m iot_analysis demo --scenario crash
```

1. **Auslöser:** Ein separat gestarteter Analyseprozess nimmt eine Nachricht entgegen. Die Demo beendet ihn während einer gezielt verlängerten Verarbeitung.
2. **Nachricht:** Sie befindet sich beim Consumer, ist aber noch nicht bestätigt.
3. **ACK/NACK:** Der beendete Prozess sendet weder ACK noch NACK. Der Broker erkennt den Verbindungsabbruch.
4. **Erneute Zustellung:** RabbitMQ reiht die unbestätigte Lieferung erneut ein. Ein neu gestarteter Consumer empfängt sie mit gesetztem `redelivered`-Flag.
5. **Fehlerbehandlung:** Die unbestätigte Lieferung bleibt beim Broker verfügbar. Der Ersatzprozess verarbeitet sie und bestätigt erst danach.
6. **DLQ:** Der Absturz allein erzeugt keinen Dead Letter. Die Nachricht gelangt nur dann dorthin, wenn sie später ohne Requeue abgelehnt wird oder als wartende Nachricht abläuft.
7. **Erholung:** Der Ersatzconsumer verarbeitet die Nachricht erfolgreich. Der Broker läuft währenddessen weiter.

**Beleg in der Demonstration:** `crash_before_ack` und `crash_redelivery_observed` zeigen dieselbe Nachrichten-ID vor und nach dem Prozessabbruch. Das Szenario prüft `redelivered=True`, die erfolgreiche Verarbeitung einer weiteren Nachricht und eine leere DLQ. Dieses Szenario erfüllt ausdrücklich den geforderten Consumer-Ausfall. Die Worker-Logs bleiben unter `.state/` erhalten.

## Szenario 3: Begrenzte lokale Verarbeitungsversuche

```text
python -m iot_analysis demo --scenario processing
```

Die Demofehler sind bewusst über Nachrichtenheader aktiviert. `x-demo-fail=transient` löst nur beim ersten lokalen Versuch einen Fehler aus. Ein weiterer Versuch verarbeitet die Nachricht erfolgreich und endet mit ACK. `x-demo-fail=permanent` schlägt in allen drei Versuchen fehl. Danach folgt NACK ohne Requeue und der DLQ-Eintrag.

Die maximale Anzahl gilt für die Verarbeitung in diesem Consumer-Aufruf. Eine permanent fehlerhafte Nachricht wird nicht unverändert unbegrenzt in die Hauptqueue zurückgeschoben. Ein Verbindungsabbruch wird von einem fachlichen Verarbeitungsfehler unterschieden.

## Szenario 4: Alarmpriorität

```text
python -m iot_analysis demo --scenario priority
```

Die Demo veröffentlicht WARNING-Alarme und CRITICAL-Alarme, bevor der Consumer sie abruft. Die Brokerqueue enthält dadurch einen Rückstau. Die Queue-Priorität bevorzugt CRITICAL mit Priorität 9 gegenüber WARNING mit Priorität 3. Prefetch 1 begrenzt die Vorabauslieferung.

Die Reihenfolge ist nur für wartende Nachrichten aussagekräftig. Ein bereits laufender WARNING-Handler kann nicht durch CRITICAL unterbrochen werden. Routing Keys allein wären kein Prioritätsmechanismus.

## Szenario 5: Nachricht läuft nach 60 Sekunden ab

```text
python -m iot_analysis demo --scenario ttl --full-ttl
```

Das Szenario publiziert eine Nachricht ohne aktiven Consumer für deren Hauptqueue. Sie wartet über die konfigurierte TTL von 60.000 ms hinaus. RabbitMQ leitet sie über den DLX in die DLQ. Der Header enthält `x-death.reason=expired`.

Es gibt keinen Consumer-ACK und keinen Consumer-NACK, da die Nachricht nicht zur Verarbeitung ausgeliefert wurde. Eine spätere reguläre Verarbeitung dieser veralteten Nachricht findet nicht statt. Die Queue bleibt für neue Nachrichten nutzbar. Eine bereits ausgelieferte, unbestätigte Nachricht unterliegt keinem nachträglichen Verarbeitungszeitlimit durch diese Queue-TTL.

Ohne `--full-ttl` setzt die Demo zusätzlich `expiration="1000"` an der Nachricht. Dann gilt die kürzere Dauer von einer Sekunde. Dieser schnelle Test demonstriert den DLX-Pfad bei Ablauf, beweist allein aber nicht die Dauer von 60 Sekunden.

## Typische Rückfragen

**Warum kommt die Nachricht beim Crash nicht sofort in die DLQ?** Die ausstehende Bestätigung erlaubt dem Broker zunächst eine erneute Zustellung. NACK ohne Requeue und TTL sind andere Mechanismen.

**Warum erst nach dem Temperaturfenster bestätigen?** Frühe ACKs würden bei einem Absturz Messwerte verlieren, die nur noch in der lokalen Aggregation existieren. Die verzögerten ACKs sichern die Quellmessungen bis zum Fensterabschluss.

**Gibt es trotzdem Duplikate?** Ja. Publish und Sensor-ACK bilden keine atomare Einheit. Der Broker kann nach einem Ausfall bereits verarbeitete Nachrichten erneut zustellen.

**Warum zählt das Monitoring nicht einfach Variablen im Analyseprozess?** Die Aufgabe fordert einen eigenen Consumer und die Bereitstellung über RabbitMQ. Die Analysen senden deshalb eigene Telemetrieereignisse.

**Was tun bei `PRECONDITION_FAILED`?** Eine bereits vorhandene Queue wurde mit anderen Argumenten erstellt. Einen neuen `IOT_NAMESPACE` wählen oder die konkret betroffene alte Projektqueue nach Prüfung gezielt entfernen. Bestehende fremde Queues niemals pauschal löschen.

**Was tun bei Verbindungsfehlern?** Brokerstatus, Port 5672, Virtual Host und Zugangsdaten prüfen. Nach fünf erfolglosen Wiederholungen endet der Dienst. Nach Behebung erneut starten.
