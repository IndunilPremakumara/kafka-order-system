#!/usr/bin/env bash
# Creates (or, with --reset, recreates) the orders and orders-dlq topics.
#
#   ./scripts/setup-topics.sh            # create if missing
#   ./scripts/setup-topics.sh --reset    # delete + recreate, for a clean demo
#
# --reset is worth running before a live demo: a leftover topic from an
# earlier run keeps its old messages *and* its old partition count, so the
# consumer would replay stale orders into the running average.
set -euo pipefail

# Git Bash on Windows rewrites /opt/... into a Windows path; this stops it.
export MSYS_NO_PATHCONV=1

KT="/opt/kafka/bin/kafka-topics.sh"
BS="localhost:9092"

if [[ "${1:-}" == "--reset" ]]; then
  echo "Deleting existing topics..."
  docker exec kafka "$KT" --bootstrap-server "$BS" --delete --topic orders || true
  docker exec kafka "$KT" --bootstrap-server "$BS" --delete --topic orders-dlq || true
  sleep 2
fi

echo "Creating topics..."
docker exec kafka "$KT" --bootstrap-server "$BS" --create --if-not-exists \
  --topic orders --partitions 3 --replication-factor 1
docker exec kafka "$KT" --bootstrap-server "$BS" --create --if-not-exists \
  --topic orders-dlq --partitions 1 --replication-factor 1

echo
docker exec kafka "$KT" --bootstrap-server "$BS" --describe --topic orders
docker exec kafka "$KT" --bootstrap-server "$BS" --describe --topic orders-dlq
