#!/bin/bash
# Helper script to load .env file with proper exports
# Usage: source load_env.sh

if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Create one using: cp .env.topstepx-direct .env"
    return 1 2>/dev/null || exit 1
fi

echo "Loading environment variables from .env..."

# Export all variables from .env
set -a
source .env
set +a

echo "✓ Environment variables loaded"
echo ""
echo "Verify with:"
echo "  echo \$TOPSTEPX_USERNAME"
echo "  echo \$TOPSTEPX_API_KEY"
echo "  echo \$TOPSTEPX_ENVIRONMENT"
