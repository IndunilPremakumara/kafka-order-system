# Creates (or, with -Reset, recreates) the orders and orders-dlq topics.
#
#   .\scripts\setup-topics.ps1           # create if missing
#   .\scripts\setup-topics.ps1 -Reset    # delete + recreate, for a clean demo
#
# -Reset is worth running before a live demo: a leftover topic from an earlier
# run keeps its old messages *and* its old partition count, so the consumer
# would replay stale orders into the running average.
param([switch]$Reset)

$ErrorActionPreference = "Stop"
$kt = "/opt/kafka/bin/kafka-topics.sh"
$bs = "localhost:9092"

if ($Reset) {
    Write-Host "Deleting existing topics..." -ForegroundColor Yellow
    docker exec kafka $kt --bootstrap-server $bs --delete --topic orders 2>$null
    docker exec kafka $kt --bootstrap-server $bs --delete --topic orders-dlq 2>$null
    Start-Sleep -Seconds 2
}

Write-Host "Creating topics..." -ForegroundColor Cyan
docker exec kafka $kt --bootstrap-server $bs --create --if-not-exists `
    --topic orders --partitions 3 --replication-factor 1
docker exec kafka $kt --bootstrap-server $bs --create --if-not-exists `
    --topic orders-dlq --partitions 1 --replication-factor 1

Write-Host "`nTopics:" -ForegroundColor Green
docker exec kafka $kt --bootstrap-server $bs --describe --topic orders
docker exec kafka $kt --bootstrap-server $bs --describe --topic orders-dlq
