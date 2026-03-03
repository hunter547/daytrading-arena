#!/bin/bash

# TopstepX Account Search - cURL Example
# This script demonstrates how to search for accounts using the TopstepX API

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Check if token is set
if [ -z "$TOPSTEPX_JWT_TOKEN" ]; then
    echo "Error: TOPSTEPX_JWT_TOKEN not set in .env file"
    exit 1
fi

# API endpoint
API_URL="https://api.topstepx.com/api/Account/search"

echo "================================================"
echo "TopstepX Account Search API Request"
echo "================================================"
echo ""
echo "Endpoint: $API_URL"
echo "Method: POST"
echo ""

# Make the request
echo "Sending request..."
echo ""

curl -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "onlyActiveAccounts": true
  }' \
  | python -m json.tool

echo ""
echo "================================================"
