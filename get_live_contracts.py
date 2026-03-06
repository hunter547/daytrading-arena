#!/usr/bin/env python3
"""Get live TopstepX contract IDs for ES and NQ."""

import asyncio
import httpx
import os
import sys


async def main():
    token = os.getenv('TOPSTEPX_JWT_TOKEN')
    if not token:
        print("Error: TOPSTEPX_JWT_TOKEN not set")
        return 1
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        print("🔍 Fetching available contracts from TopstepX...\n")
        
        response = await client.post(
            'https://api.topstepx.com/api/Contract/available',
            headers={
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            },
            json={'live': False}
        )
        
        if response.status_code != 200:
            print(f"❌ Error: {response.status_code}")
            print(response.text[:500])
            return 1
        
        data = response.json()
        contracts = data.get('contracts', [])
        
        print(f"✅ Found {len(contracts)} available contracts\n")
        print("=" * 80)
        
        # Find ES (full-sized E-mini S&P 500)
        es_contracts = []
        for c in contracts:
            cid = c.get('id', '')
            name = c.get('name', '')
            # Look for .ES. but not .MES. (micro) or .ENQ.
            if '.ES.' in cid and '.MES.' not in cid:
                es_contracts.append((cid, name))
        
        print("\n📊 E-MINI S&P 500 (ES) CONTRACTS:")
        print("-" * 80)
        if es_contracts:
            for cid, name in es_contracts:
                print(f"  ✓ {cid:35s} | {name}")
        else:
            print("  ❌ No full-sized ES contracts found")
            print("\n  Available ES-related contracts:")
            for c in contracts:
                cid = c.get('id', '')
                if 'ES' in cid:
                    print(f"     {cid:35s} | {c.get('name', '')}")

        print("\n" + "=" * 80)
        print("💡 TO USE THESE CONTRACTS:")
        print("=" * 80)

        if es_contracts:
            es_id = es_contracts[0][0]

            print(f"\n./run.sh python topstepx_tick_viewer.py")
            print(f"\n# Or set in .env:")
            print(f"TOPSTEPX_SYMBOLS={es_id}")
        
        print("\n")
        return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
