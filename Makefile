.PHONY: help build up down logs restart ps clean build-no-cache stop

# Default target
help:
	@echo "Futures Trading System - Docker Compose Commands"
	@echo ""
	@echo "Quick Start:"
	@echo "  make build          - Build all Docker images"
	@echo "  make up             - Start all services (without response viewer)"
	@echo "  make up-monitor     - Start all services including response viewer"
	@echo "  make down           - Stop all services"
	@echo ""
	@echo "Monitoring:"
	@echo "  make logs           - View logs from all services"
	@echo "  make logs-agent     - View agent logs"
	@echo "  make logs-market    - View market connector logs"
	@echo "  make logs-chatnode  - View chatnode logs"
	@echo "  make logs-viewer    - View response viewer logs"
	@echo "  make ps             - Show service status"
	@echo ""
	@echo "Maintenance:"
	@echo "  make restart        - Restart all services"
	@echo "  make rebuild        - Rebuild and restart services"
	@echo "  make stop           - Stop services (keep containers)"
	@echo "  make clean          - Remove all containers, networks, volumes"
	@echo "  make clean-all      - Remove everything including images"
	@echo ""
	@echo "Dashboard:"
	@echo "  make dashboard      - Open dashboard in browser"
	@echo ""

# Build Docker images
build:
	docker-compose build

# Build without cache
build-no-cache:
	docker-compose build --no-cache

# Start all services (without response viewer)
up:
	docker-compose up -d
	@echo ""
	@echo "✅ All services started!"
	@echo "📊 Dashboard: http://localhost:8501"
	@echo ""
	@echo "View logs: make logs"
	@echo "Check status: make ps"
	@echo ""

# Start all services including response viewer
up-monitor:
	docker-compose --profile monitoring up -d
	@echo ""
	@echo "✅ All services started (including response viewer)!"
	@echo "📊 Dashboard: http://localhost:8501"
	@echo ""
	@echo "View agent activity: make logs-viewer"
	@echo "View logs: make logs"
	@echo ""

# Stop all services
down:
	docker-compose down

# Stop services (keep containers)
stop:
	docker-compose stop

# View logs from all services
logs:
	docker-compose logs -f

# View specific service logs
logs-agent:
	docker-compose logs -f agent

logs-market:
	docker-compose logs -f market-connector

logs-chatnode:
	docker-compose logs -f chatnode

logs-dashboard:
	docker-compose logs -f dashboard

logs-viewer:
	docker-compose logs -f response-viewer

logs-kafka:
	docker-compose logs -f kafka

# Show service status
ps:
	docker-compose ps

# Restart all services
restart:
	docker-compose restart

# Rebuild and restart
rebuild:
	docker-compose up -d --build
	@echo ""
	@echo "✅ Services rebuilt and restarted!"
	@echo "📊 Dashboard: http://localhost:8501"
	@echo ""

# Clean up containers and networks
clean:
	docker-compose down -v
	@echo "✅ All containers, networks, and volumes removed"

# Clean up everything including images
clean-all:
	docker-compose down -v --rmi all
	@echo "✅ Everything removed (containers, networks, volumes, images)"

# Open dashboard in browser
dashboard:
	@echo "Opening dashboard at http://localhost:8501"
	@command -v open >/dev/null 2>&1 && open http://localhost:8501 || \
	command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8501 || \
	echo "Please open http://localhost:8501 in your browser"

# Check environment setup
check-env:
	@echo "Checking environment configuration..."
	@test -f .env || (echo "❌ .env file not found. Copy .env.example to .env" && exit 1)
	@grep -q "OPENAI_API_KEY=" .env || (echo "⚠️  OPENAI_API_KEY not set in .env" && exit 1)
	@grep -q "TOPSTEPX_USERNAME=" .env || (echo "⚠️  TOPSTEPX_USERNAME not set in .env" && exit 1)
	@grep -q "TOPSTEPX_API_KEY=" .env || (echo "⚠️  TOPSTEPX_API_KEY not set in .env" && exit 1)
	@echo "✅ Environment configuration looks good!"

# Full system check
health:
	@echo "Checking system health..."
	@docker ps >/dev/null 2>&1 || (echo "❌ Docker is not running" && exit 1)
	@echo "✅ Docker is running"
	@docker-compose ps | grep -q "Up" && echo "✅ Services are running" || echo "⚠️  No services running. Use 'make up' to start."
	@curl -s http://localhost:8501 >/dev/null && echo "✅ Dashboard is accessible" || echo "⚠️  Dashboard not accessible"

# Quick restart of agent (useful during development)
restart-agent:
	docker-compose restart agent
	@echo "✅ Agent restarted"

# Quick restart of market connector
restart-market:
	docker-compose restart market-connector
	@echo "✅ Market connector restarted"

# Show Kafka topics
kafka-topics:
	docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
