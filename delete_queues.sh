#!/usr/bin/env bash
# Loescht ALLE Queues im RabbitMQ (Exchanges bleiben bestehen).
# Vorher alle Consumer und den Producer beenden, sonst deklarieren sie Queues neu.
#
# Aufruf: ./delete_queues.sh

export PATH="$PATH:$(brew --prefix rabbitmq 2>/dev/null)/sbin"
command -v rabbitmqctl >/dev/null || { echo "rabbitmqctl nicht gefunden"; exit 1; }

queues=$(rabbitmqctl list_queues name --quiet --no-table-headers 2>/dev/null) \
  || { echo "RabbitMQ laeuft nicht oder ist nicht erreichbar"; exit 1; }

[ -z "$queues" ] && { echo "Keine Queues vorhanden"; exit 0; }

while read -r q; do
  [ -z "$q" ] && continue
  if rabbitmqctl delete_queue "$q" >/dev/null 2>&1; then
    echo "geloescht: $q"
  else
    echo "FEHLER beim Loeschen: $q"
  fi
done <<< "$queues"
