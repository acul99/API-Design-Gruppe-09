#!/usr/bin/env bash
# Startet das ganze System, beendet nach 10 s den Alarm-Consumer hart (kill -9)
# und startet ihn nach weiteren 10 s neu (Resilienztest A: Consumer-Ausfall).
#
# Aufruf:  ./demo_resilienz.sh        (Ctrl+C beendet alles)
# Optional: KILL_AFTER=10 DOWN_FOR=10 ALARM_WORK_MS=3000 ./demo_resilienz.sh
# Beim Beenden (Ctrl+C) werden alle Prozesse gestoppt und ALLE Queues geloescht.
# Schau dir vorher Queues und DLQ im Management UI an, danach ist alles weg.

cd "$(dirname "$0")" || exit 1
[ -f .venv/bin/activate ] && source .venv/bin/activate

KILL_AFTER=${KILL_AFTER:-10}      # Sekunden bis zum Abbruch
DOWN_FOR=${DOWN_FOR:-10}          # Sekunden Ausfall, muss unter der TTL von 60 s liegen
export ALARM_WORK_MS=${ALARM_WORK_MS:-3000}   # lange Verarbeitung: beim Kill ist ein Alarm "in flight"

# RabbitMQ (Homebrew) starten und auf Port 5672 warten. rabbitmqctl liegt nicht im PATH.
export PATH="$PATH:$(brew --prefix rabbitmq 2>/dev/null)/sbin"
echo "[$(date +%T)] Starte RabbitMQ (brew services start rabbitmq)"
brew services start rabbitmq || { echo "RabbitMQ konnte nicht gestartet werden"; exit 1; }
for i in $(seq 1 60); do
  nc -z localhost 5672 2>/dev/null && break
  [ "$i" -eq 60 ] && { echo "RabbitMQ antwortet nach 60 s nicht auf Port 5672"; exit 1; }
  sleep 1
done
echo "[$(date +%T)] RabbitMQ erreichbar"

mkdir -p logs
: > logs/alarm.log; : > logs/climate.log; : > logs/pressure.log
: > logs/monitor.log; : > logs/producer.log

delete_all_queues() {
  command -v rabbitmqctl >/dev/null || { echo "rabbitmqctl nicht gefunden, Queues bleiben"; return 0; }
  rabbitmqctl list_queues name --quiet --no-table-headers 2>/dev/null | while read -r q; do
    [ -n "$q" ] && rabbitmqctl delete_queue "$q" >/dev/null 2>&1 && echo "    Queue geloescht: $q"
  done
}

PIDS=()
cleanup() {
  trap '' INT TERM
  echo; echo "[$(date +%T)] Beende alle Prozesse ..."
  kill "${PIDS[@]}" "$TAIL_PID" 2>/dev/null
  wait "${PIDS[@]}" 2>/dev/null   # erst loeschen, wenn niemand mehr Queues neu deklariert
  echo "[$(date +%T)] Loesche alle Queues"
  delete_all_queues
  exit 0
}
trap cleanup INT TERM

# Einmalig sinnvoll, wenn noch Queues einer aelteren Version existieren (PRECONDITION_FAILED):
#   CLEAN_START=1 ./demo_resilienz.sh
if [ "${CLEAN_START:-0}" = "1" ]; then
  echo "[$(date +%T)] CLEAN_START: loesche vorhandene Queues"
  delete_all_queues
fi

queue_status() {
  command -v rabbitmqctl >/dev/null || return 0
  rabbitmqctl list_queues name messages_ready messages_unacknowledged --quiet 2>/dev/null \
    | grep -E "^(alarms|iot.dlq)" | sed 's/^/    /'
}

echo "[$(date +%T)] Starte Consumer und Producer"
python analyse_consumer.py climate  >> logs/climate.log  2>&1 & PIDS+=($!)
python analyse_consumer.py pressure >> logs/pressure.log 2>&1 & PIDS+=($!)
python monitoring_consumer.py       >> logs/monitor.log  2>&1 & PIDS+=($!)
python alarm_consumer.py            >> logs/alarm.log    2>&1 & ALARM_PID=$!; PIDS+=($ALARM_PID)
sleep 2
python sensorProducer.py            >> logs/producer.log 2>&1 & PIDS+=($!)

# Ausgaben von Alarm- und Monitoring-Consumer live anzeigen
tail -n0 -f logs/alarm.log logs/monitor.log &
TAIL_PID=$!

sleep "$KILL_AFTER"
echo; echo "[$(date +%T)] >>> kill -9 alarm_consumer (PID $ALARM_PID)"
echo "  Queues davor (messages_ready / unacked):"; queue_status
kill -9 "$ALARM_PID" 2>/dev/null

sleep "$DOWN_FOR"
echo; echo "[$(date +%T)] >>> Alarm-Consumer ist seit ${DOWN_FOR}s aus"
echo "  Queues vor dem Neustart (Alarme haben sich gesammelt):"; queue_status
python alarm_consumer.py >> logs/alarm.log 2>&1 & ALARM_PID=$!; PIDS+=($ALARM_PID)
echo "[$(date +%T)] >>> Alarm-Consumer neu gestartet (PID $ALARM_PID), Ctrl+C beendet alles"; echo

wait "$TAIL_PID"
