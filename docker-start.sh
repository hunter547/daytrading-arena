#!/bin/bash
# Quick start script for Docker Compose setup

set -e

echo "🚀 Futures Trading System - Docker Setup"
echo ""

# Check Docker
if ! docker ps &> /dev/null; then
    echo "❌ Docker is not running. Please start Docker Desktop."
    exit 1
fi
echo "✅ Docker is running"

# Check .env file
if [ ! -f .env ]; then
    echo "❌ .env file not found"
    echo ""
    echo "Please create .env file from .env.example:"
    echo "  cp .env.example .env"
    echo "  # Then edit .env and add your API keys"
    exit 1
fi
echo "✅ .env file found"

# Check required environment variables
source .env
if [ -z "$OPENAI_API_KEY" ]; then
    echo "⚠️  Warning: OPENAI_API_KEY not set in .env"
fi
if [ -z "$TOPSTEPX_USERNAME" ]; then
    echo "⚠️  Warning: TOPSTEPX_USERNAME not set in .env"
fi
if [ -z "$TOPSTEPX_API_KEY" ]; then
    echo "⚠️  Warning: TOPSTEPX_API_KEY not set in .env"
fi

echo ""
echo "Building Docker images (this may take a few minutes)..."
docker-compose build

echo ""
echo "Starting all services..."
echo ""

# Ask user if they want response viewer
read -p "Start with Response Viewer? (shows live agent reasoning) [y/N]: " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker-compose --profile monitoring up -d
    VIEWER_MSG="📺 Response Viewer: docker-compose logs -f response-viewer"
else
    docker-compose up -d
    VIEWER_MSG="💡 To start Response Viewer later: docker-compose --profile monitoring up -d response-viewer"
fi

echo ""
echo "⏳ Waiting for services to start..."
sleep 5

# Check service health
echo ""
echo "📊 Service Status:"
docker-compose ps

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ System Started Successfully!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "🌐 Dashboard:     http://localhost:8501"
echo ""
echo "📋 Useful Commands:"
echo "  View all logs:       docker-compose logs -f"
echo "  View agent logs:     docker-compose logs -f agent"
echo "  View market data:    docker-compose logs -f market-connector"
echo "  $VIEWER_MSG"
echo "  Check status:        docker-compose ps"
echo "  Stop services:       docker-compose down"
echo ""
echo "📖 Full docs: See DOCKER_SETUP.md"
echo ""
