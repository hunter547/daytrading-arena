#!/bin/bash
# Pre-create all required Kafka topics on startup.
# Runs inside the Kafka container via docker-compose.

BROKER="localhost:9092"

TOPICS=(
  "agent_router.input"
  "agent_router.output"
  "market_data.futures_prices"
  "tool_node.topstepx_buy.request"
  "tool_node.topstepx_buy.result"
  "tool_node.topstepx_sell.request"
  "tool_node.topstepx_sell.result"
  "tool_node.topstepx_close.request"
  "tool_node.topstepx_close.result"
  "tool_node.topstepx_portfolio.request"
  "tool_node.topstepx_portfolio.result"
  "tool_node.report_sentiment.request"
  "tool_node.report_sentiment.result"
  "tool_node.calculator.request"
  "tool_node.calculator.result"
)

echo "Waiting for Kafka to be ready..."
cub kafka-ready -b "$BROKER" 1 60 2>/dev/null || sleep 10

for TOPIC in "${TOPICS[@]}"; do
  kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists \
    --topic "$TOPIC" --partitions 1 --replication-factor 1 2>/dev/null
  echo "  Topic: $TOPIC"
done

echo "All topics created."
