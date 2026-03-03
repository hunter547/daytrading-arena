# TopstepX API - cURL Examples

Complete collection of cURL commands for interacting with the TopstepX API.

## Prerequisites

Set your JWT token as an environment variable:
```bash
export TOPSTEPX_JWT_TOKEN="your_jwt_token_here"
```

Or load from `.env`:
```bash
source .env
```

---

## Authentication

### Get JWT Token (Login with API Key)
```bash
curl -X POST "https://api.topstepx.com/api/Auth/loginKey" \
  -H "Content-Type: application/json" \
  -d '{
    "userName": "your_username",
    "apiKey": "your_api_key"
  }' | jq .
```

**Response:**
```json
{
  "success": true,
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "errorCode": 0,
  "errorMessage": null
}
```

### Validate Token
```bash
curl -X GET "https://api.topstepx.com/api/Auth/validateSession" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" | jq .
```

---

## Account Management

### 1. Search Accounts (Active Only)
```bash
curl -X POST "https://api.topstepx.com/api/Account/search" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "onlyActiveAccounts": true
  }' | jq .
```

**Response:**
```json
{
  "success": true,
  "accounts": [
    {
      "id": 19424999,
      "name": "PRAC-V2-157469-77399797",
      "canTrade": true,
      "balance": 150000.0,
      "isVisible": true,
      "simulated": true
    },
    {
      "id": 19465121,
      "name": "50KTC-V2-157469-24589604",
      "canTrade": true,
      "balance": 50839.2,
      "isVisible": true,
      "simulated": true
    }
  ],
  "errorCode": 0,
  "errorMessage": null
}
```

### 2. Search All Accounts (Including Inactive)
```bash
curl -X POST "https://api.topstepx.com/api/Account/search" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "onlyActiveAccounts": false
  }' | jq .
```

---

## Positions

### 1. Get Open Positions for Account
```bash
ACCOUNT_ID=19424999

curl -X POST "https://api.topstepx.com/api/Position/searchOpen" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d "{
    \"accountId\": $ACCOUNT_ID
  }" | jq .
```

**Response:**
```json
{
  "success": true,
  "positions": [
    {
      "id": 12345,
      "accountId": 19424999,
      "contractId": "CON.F.US.MES.H26",
      "creationTimestamp": "2026-02-26T10:00:00Z",
      "type": 0,
      "size": 1,
      "averagePrice": 5800.50
    }
  ],
  "errorCode": 0,
  "errorMessage": null
}
```

### 2. Close Position
```bash
curl -X POST "https://api.topstepx.com/api/Position/closeContract" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "accountId": 19424999,
    "contractId": "CON.F.US.MES.H26"
  }' | jq .
```

### 3. Partial Close Position
```bash
curl -X POST "https://api.topstepx.com/api/Position/partialCloseContract" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "accountId": 19424999,
    "contractId": "CON.F.US.MES.H26",
    "size": 1
  }' | jq .
```

---

## Orders

### 1. Place Market Order (Buy)
```bash
curl -X POST "https://api.topstepx.com/api/Order/place" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "accountId": 19424999,
    "contractId": "CON.F.US.MES.H26",
    "type": 2,
    "side": 0,
    "size": 1
  }' | jq .
```

**Parameters:**
- `type`: 1=Limit, 2=Market, 3=Stop, 4=StopLimit
- `side`: 0=Buy, 1=Sell
- `size`: Number of contracts

**Response:**
```json
{
  "success": true,
  "orderId": 67890,
  "errorCode": 0,
  "errorMessage": null
}
```

### 2. Place Limit Order
```bash
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
  }' | jq .
```

### 3. Place Stop Order
```bash
curl -X POST "https://api.topstepx.com/api/Order/place" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "accountId": 19424999,
    "contractId": "CON.F.US.MES.H26",
    "type": 3,
    "side": 1,
    "size": 1,
    "stopPrice": 5750.00
  }' | jq .
```

### 4. Search Open Orders
```bash
curl -X POST "https://api.topstepx.com/api/Order/searchOpen" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "accountId": 19424999
  }' | jq .
```

### 5. Cancel Order
```bash
ORDER_ID=67890

curl -X POST "https://api.topstepx.com/api/Order/cancel" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d "{
    \"accountId\": 19424999,
    \"orderId\": $ORDER_ID
  }" | jq .
```

### 6. Modify Order
```bash
curl -X POST "https://api.topstepx.com/api/Order/modify" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "accountId": 19424999,
    "orderId": 67890,
    "size": 2,
    "limitPrice": 5795.00
  }' | jq .
```

---

## Contracts

### 1. Search Contracts by Text
```bash
curl -X POST "https://api.topstepx.com/api/Market/contracts/search" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "text": "MES"
  }' | jq .
```

### 2. Get Available Contracts
```bash
curl -X GET "https://api.topstepx.com/api/Market/contracts/available" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" | jq .
```

---

## Market History

### Get Historical Bars (Candles)
```bash
curl -X POST "https://api.topstepx.com/api/History/retrieveBars" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "contractId": "CON.F.US.MES.H26",
    "barUnit": 2,
    "barLength": 5,
    "numberOfBars": 100
  }' | jq .
```

**Parameters:**
- `barUnit`: 1=Second, 2=Minute, 3=Hour, 4=Day, 5=Week, 6=Month
- `barLength`: Number of units (e.g., 5 for 5-minute bars)
- `numberOfBars`: How many bars to retrieve

---

## Trade History

### Search Trades for Account
```bash
curl -X POST "https://api.topstepx.com/api/Trade/search" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{
    "accountId": 19424999,
    "startDateTime": "2026-02-01T00:00:00Z",
    "endDateTime": "2026-02-26T23:59:59Z"
  }' | jq .
```

---

## Useful Shell Functions

Add these to your `~/.bashrc` or `~/.zshrc`:

```bash
# TopstepX API helper functions
export TOPSTEPX_API="https://api.topstepx.com"

# Get accounts
tsx_accounts() {
    curl -s -X POST "$TOPSTEPX_API/api/Account/search" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
      -d '{"onlyActiveAccounts": true}' | jq .
}

# Get positions for account
tsx_positions() {
    local account_id=$1
    curl -s -X POST "$TOPSTEPX_API/api/Position/searchOpen" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
      -d "{\"accountId\": $account_id}" | jq .
}

# Place market buy order
tsx_buy() {
    local account_id=$1
    local contract=$2
    local size=$3
    curl -s -X POST "$TOPSTEPX_API/api/Order/place" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
      -d "{
        \"accountId\": $account_id,
        \"contractId\": \"$contract\",
        \"type\": 2,
        \"side\": 0,
        \"size\": $size
      }" | jq .
}

# Place market sell order
tsx_sell() {
    local account_id=$1
    local contract=$2
    local size=$3
    curl -s -X POST "$TOPSTEPX_API/api/Order/place" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
      -d "{
        \"accountId\": $account_id,
        \"contractId\": \"$contract\",
        \"type\": 2,
        \"side\": 1,
        \"size\": $size
      }" | jq .
}
```

**Usage:**
```bash
# Get all accounts
tsx_accounts

# Get positions for account 19424999
tsx_positions 19424999

# Buy 1 MES contract
tsx_buy 19424999 "CON.F.US.MES.H26" 1

# Sell 1 MES contract
tsx_sell 19424999 "CON.F.US.MES.H26" 1
```

---

## Common Contracts

| Contract ID | Description | Tick Value |
|------------|-------------|------------|
| `CON.F.US.MES.H26` | Micro E-mini S&P 500 | $1.25 |
| `CON.F.US.MNQ.H26` | Micro E-mini Nasdaq-100 | $0.50 |
| `CON.F.US.ES.H26` | E-mini S&P 500 | $12.50 |
| `CON.F.US.NQ.H26` | E-mini Nasdaq-100 | $5.00 |

---

## Testing Script

Use the provided `test_account_search.sh`:

```bash
./test_account_search.sh
```

Or create your own test script:

```bash
#!/bin/bash
source .env

echo "Testing TopstepX API..."

# Test 1: Validate token
echo -e "\n1. Validating token..."
curl -s -X GET "$TOPSTEPX_API/api/Auth/validateSession" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" | jq '.success'

# Test 2: Get accounts
echo -e "\n2. Fetching accounts..."
curl -s -X POST "$TOPSTEPX_API/api/Account/search" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOPSTEPX_JWT_TOKEN" \
  -d '{"onlyActiveAccounts": true}' | jq '.accounts | length'

echo -e "\n✓ Tests complete"
```

---

## Error Handling

**Common Error Responses:**

```json
{
  "success": false,
  "errorCode": 1,
  "errorMessage": "Invalid token"
}
```

**Error Codes:**
- `0`: Success
- `1`: Authentication error
- `2`: Validation error
- `3`: Invalid credentials
- `4`: Insufficient permissions

---

## Rate Limits

- REST API: ~100 requests per minute
- WebSocket: Unlimited subscriptions
- Recommended: Use WebSocket for real-time data, REST for commands

---

## Resources

- API Documentation: https://api.topstepx.com/swagger
- Get JWT Token: `./run.sh python topstepx_auth.py`
- Python Client: `./run.sh python topstepx_account.py`
