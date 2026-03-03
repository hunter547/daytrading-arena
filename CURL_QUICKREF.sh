#!/bin/bash

# TopstepX API - Quick Reference
# Copy these commands and replace variables as needed

# Set your token
export TOPSTEPX_JWT_TOKEN="your_token_here"

# ============================================================================
# ACCOUNTS
# ============================================================================

# Get all active accounts
curl -X POST "https://api.topstepx.com/api/Account/search" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{"onlyActiveAccounts": true}'

# ============================================================================
# POSITIONS
# ============================================================================

# Get open positions for account
curl -X POST "https://api.topstepx.com/api/Position/searchOpen" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{"accountId": 19424999}'

# ============================================================================
# ORDERS
# ============================================================================

# Place MARKET BUY order
curl -X POST "https://api.topstepx.com/api/Order/place" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "accountId": 19424999,
    "contractId": "CON.F.US.MES.H26",
    "type": 2,
    "side": 0,
    "size": 1
  }'

# Place MARKET SELL order
curl -X POST "https://api.topstepx.com/api/Order/place" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "accountId": 19424999,
    "contractId": "CON.F.US.MES.H26",
    "type": 2,
    "side": 1,
    "size": 1
  }'

# Place LIMIT BUY order
curl -X POST "https://api.topstepx.com/api/Order/place" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "accountId": 19424999,
    "contractId": "CON.F.US.MES.H26",
    "type": 1,
    "side": 0,
    "size": 1,
    "limitPrice": 5800.00
  }'

# Get open orders
curl -X POST "https://api.topstepx.com/api/Order/searchOpen" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{"accountId": 19424999}'

# Cancel order
curl -X POST "https://api.topstepx.com/api/Order/cancel" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{"accountId": 19424999, "orderId": 12345}'

# ============================================================================
# CONTRACTS
# ============================================================================

# Search contracts
curl -X POST "https://api.topstepx.com/api/Market/contracts/search" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{"text": "MES"}'

# Get available contracts
curl -X GET "https://api.topstepx.com/api/Market/contracts/available" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN"

# ============================================================================
# NOTES
# ============================================================================
# 
# Order Types:
#   1 = Limit
#   2 = Market
#   3 = Stop
#   4 = StopLimit
#
# Order Sides:
#   0 = Buy
#   1 = Sell
#
# Common Contracts:
#   CON.F.US.MES.H26 - Micro E-mini S&P 500
#   CON.F.US.MNQ.H26 - Micro E-mini Nasdaq-100
#   CON.F.US.ES.H26  - E-mini S&P 500
#   CON.F.US.NQ.H26  - E-mini Nasdaq-100
#
# Practice Account ID: 19424999
#
