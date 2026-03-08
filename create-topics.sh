#!/bin/bash
# Pre-create all required Kafka topics on startup.
# Runs inside the Kafka container via docker-compose.

BROKER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

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

# ChatNode topics (2 partitions each) — read model names from agents.yml
# agents.yml is mounted at /agents.yml in the init-topics container
AGENTS_FILE="/agents.yml"
if [ -f "$AGENTS_FILE" ]; then
  # Extract model names from the "models:" section (top-level keys with model_id children)
  MODELS=$(awk '/^models:/{found=1;next} /^[a-z]/{found=0} found && /^  [a-z]/{gsub(/[: ]/,""); print}' "$AGENTS_FILE")
  for MODEL in $MODELS; do
    TOPIC="ai_prompted.${MODEL}"
    kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists \
      --topic "$TOPIC" --partitions 2 --replication-factor 1 2>/dev/null
    echo "  Topic: $TOPIC (2 partitions)"
  done
else
  # Fallback: create default chatnode topic
  kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists \
    --topic "ai_prompted.gpt5-nano" --partitions 2 --replication-factor 1 2>/dev/null
  echo "  Topic: ai_prompted.gpt5-nano (2 partitions)"
fi

echo "All topics created."
