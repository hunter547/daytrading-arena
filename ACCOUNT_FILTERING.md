# TopstepX Account Filtering

## Overview

The dashboard automatically filters TopstepX accounts to show only those eligible for trading based on their **Major Loss Limit (MLL)** status.

## Major Loss Limits by Account Type

| Account Type | Starting Balance | Major Loss Limit | Minimum Balance Required |
|--------------|------------------|------------------|-------------------------|
| **50K Challenge** | $50,000 | $2,000 | **$48,000** |
| **100K Challenge** | $100,000 | $3,000 | **$97,000** |
| **150K Challenge** | $150,000 | $4,500 | **$145,500** |
| **Practice (PRAC-*)** | Varies | None | Always eligible |

## Filtering Logic

Accounts are automatically filtered through multiple criteria:

### 1. API-Level Filters
- `onlyActiveAccounts: true` - Only active accounts returned from API
- `canTrade: true` - Account has trading permissions

### 2. Client-Level Filters
- **Balance Check**: Account balance must be above MLL threshold
- **Visibility Check**: Account must have `isVisible: true`
- **Practice Exception**: Practice accounts (PRAC-*) always pass balance checks

## Your Current Accounts

Based on your latest data:

### ✅ ELIGIBLE (4 accounts)
1. **50KTC-V2-157469-53602855** - Balance: $49,382.52 (Above $48K MLL)
2. **50KTC-V2-157469-95378128** - Balance: $49,127.60 (Above $48K MLL)
3. **PRAC-V2-157469-77399797** - Balance: $150,000 (Practice - always eligible)
4. **50KTC-V2-157469-24589604** - Balance: $50,839.20 (Above $48K MLL, has 1 position)

### ❌ INELIGIBLE (2 accounts - below MLL)
1. **50KTC-V2-157469-92441086** - Balance: $47,912.20 (Below $48K threshold)
2. **50KTC-V2-157469-42174448** - Balance: $47,659.80 (Below $48K threshold)

## Code Implementation

The filtering is implemented in `topstepx_account.py`:

```python
# Automatic MLL detection
if account_name.startswith("50K"):
    starting_balance = 50000
    major_loss_limit = 2000  # $2K MLL
    min_balance = 48000
    
elif account_name.startswith("100K"):
    starting_balance = 100000
    major_loss_limit = 3000  # $3K MLL
    min_balance = 97000
    
elif account_name.startswith("150K"):
    starting_balance = 150000
    major_loss_limit = 4500  # $4.5K MLL
    min_balance = 145500

# Practice accounts always pass
if "PRAC" in account_name:
    # Always eligible
    pass
```

## Viewing Filtered Accounts

### See All Eligible Accounts
```bash
./run.sh python topstepx_account.py
```

### Check Specific Account
```bash
./run.sh python topstepx_account.py --account-id 19424999
```

### Dashboard View
```bash
# Shows only eligible accounts
./run.sh python tools_and_dashboard.py --bootstrap-servers localhost:9092
```

## What Happens When an Account Falls Below MLL

When an account balance drops below its MLL threshold:

1. **Automatically Filtered Out**: Account disappears from dashboard
2. **Cannot Trade**: Even if `canTrade: true`, balance check prevents display
3. **Logged**: Info message shows why account was skipped
4. **Practice Unaffected**: Practice accounts are never filtered by balance

## Logs

When accounts are filtered, you'll see log messages like:

```
[INFO] Skipping account 50KTC-V2-157469-92441086 (ID: 18987830): 
Balance $47,912.20 below MLL threshold $48,000.00 
(Starting: $50,000.00, MLL: $2,000.00)
```

## Modifying Filter Behavior

If you need to change the filtering logic, edit `topstepx_account.py`:

```python
async def get_accounts(self, only_active: bool = True) -> list[TopstepXAccount]:
    """
    Modify the logic in this method to:
    - Add new account types
    - Change MLL thresholds
    - Add additional filters
    """
```

## Benefits

✅ **Safety**: Prevents displaying accounts that can't trade
✅ **Clarity**: Shows only actionable accounts
✅ **Automatic**: No manual filtering required
✅ **Accurate**: Respects TopstepX MLL rules
✅ **Flexible**: Practice accounts always available for testing

## Testing Different Thresholds

For debugging or testing, you can temporarily modify thresholds:

```python
# In topstepx_account.py
if account_name.startswith("50K"):
    starting_balance = 50000
    major_loss_limit = 2000  # Change this for testing
```

Then restart your dashboard to see the effects.
