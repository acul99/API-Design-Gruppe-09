cd ~/API-Projekt1
for cmd in "python analyse_consumer.py climate" \
           "python analyse_consumer.py pressure" \
           "python alarm_consumer.py" \
           "python monitoring_consumer.py" \
           "python sensorProducer.py" \
           # comment "faultyProducer" out for normal data
           "python faultyProducer.py"
           ; do
  osascript -e "tell application \"Terminal\" to do script \"cd $PWD && source .venv/bin/activate && $cmd\""
done
