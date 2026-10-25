Fehlerszenario 1: alarm_consumer fällt nach x Sekunden für y Sekunden aus (Neustart nach y Sekunden).

1. Das Demo-Skript beendet den alarm_consumer nach x Sekunden hart mit kill -9
   (wie ein Absturz) und startet ihn nach y Sekunden neu.
2. Die Alarme, die schon in der Queue "alarms" warten, bleiben dort. Der Alarm, der gerade
   in Arbeit war, ist unbestätigt und wird vom Broker zurück in die Queue gestellt.
   Neue Alarme sammeln sich weiter an.
3. Weder ACK noch NACK, der Prozess ist tot. Der Broker behandelt es wie ein NACK mit Requeue.
4. Ja. Nach dem Neustart wird der Alarm mit redelivered=True erneut zugestellt und
   vollständig neu verarbeitet, danach folgen die gesammelten Alarme, CRITICAL zuerst.
5. Der Alarm-Consumer bestätigt manuell und erst nach erfolgreicher Verarbeitung (At-least-once).
   prefetch_count=1 begrenzt das auf höchstens einen unbestätigten Alarm.
   Doppelte Alarme werden über source_message_id und Severity erkannt.
6. In diesem Szenario nicht, solange y kleiner als die TTL von 60 s ist.
   Bei längerem Ausfall laufen die Alarme ab und gehen über die DLX in die DLQ.
7. Ja. Producer und Analyse-Consumer laufen weiter, es geht nichts verloren, und die
   Queue wird nach dem Neustart in Prioritätsreihenfolge abgearbeitet.
