#!/usr/bin/env python3
"""Test script to inspect market date fields from Polymarket and Kalshi APIs"""
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import requests
    import json
    from datetime import datetime, timezone
    from dotenv import load_dotenv
    load_dotenv()
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Run: pip install -r requirements.txt")
    sys.exit(1)

def test_polymarket_dates():
    """Fetch and inspect Polymarket market dates"""
    print("\n" + "="*80)
    print("TESTING POLYMARKET API")
    print("="*80 + "\n")

    url = "https://gamma-api.polymarket.com/markets"
    params = {
        "limit": 20,
        "offset": 0,
        "archived": "false"  # Only active markets
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        markets = response.json()

        print(f"Fetched {len(markets)} markets\n")

        # Look specifically for markets with "2024" in the title
        markets_2024 = [m for m in markets if "2024" in m.get("question", "")]

        if markets_2024:
            print(f"⚠️  FOUND {len(markets_2024)} MARKETS WITH '2024' IN TITLE:\n")

            for market in markets_2024[:5]:  # Show first 5
                print("-" * 80)
                print(f"Question: {market.get('question', 'N/A')[:100]}")
                print(f"Active: {market.get('active')}")
                print(f"Closed: {market.get('closed')}")
                print(f"Archived: {market.get('archived')}")

                # Date fields
                end_date = market.get('end_date_iso')
                close_time = market.get('close_time')
                start_date = market.get('start_date_iso')

                print(f"\nDATE FIELDS:")
                print(f"  end_date_iso: {end_date}")
                print(f"  start_date_iso: {start_date}")
                print(f"  close_time: {close_time}")

                # Parse and check dates
                if end_date:
                    try:
                        end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                        now = datetime.now(timezone.utc)
                        is_past = end_dt < now
                        print(f"  → End date parsed: {end_dt}")
                        print(f"  → Is in the past? {is_past}")
                        print(f"  → Days until/since end: {(end_dt - now).days}")
                    except Exception as e:
                        print(f"  → Error parsing end_date: {e}")

                # Volume info
                print(f"\nVOLUME: ${market.get('volume', 0):,.0f}")

                # All available fields
                print(f"\nALL FIELDS: {list(market.keys())}")
                print()
        else:
            print("✅ No markets with '2024' in title found in first 20\n")

        # Show a few sample markets regardless
        print("\n" + "-"*80)
        print("SAMPLE OF FIRST 3 MARKETS (regardless of year):\n")
        for i, market in enumerate(markets[:3], 1):
            print(f"{i}. {market.get('question', 'N/A')[:80]}")
            print(f"   Active: {market.get('active')} | end_date_iso: {market.get('end_date_iso')}")
            print(f"   Volume: ${market.get('volume', 0):,.0f}\n")

    except Exception as e:
        print(f"❌ Error fetching Polymarket: {e}")


def test_kalshi_dates():
    """Fetch and inspect Kalshi market dates"""
    print("\n" + "="*80)
    print("TESTING KALSHI API")
    print("="*80 + "\n")

    # Try to authenticate
    email = os.getenv("KALSHI_EMAIL")
    password = os.getenv("KALSHI_PASSWORD")

    base_url = "https://api.elections.kalshi.com/trade-api/v2"
    headers = {}

    # Try to login if credentials available
    if email and password:
        try:
            login_url = f"{base_url}/login"
            login_data = {"email": email, "password": password}
            resp = requests.post(login_url, json=login_data, timeout=10)
            resp.raise_for_status()
            token = resp.json().get("token")
            if token:
                headers["Authorization"] = f"Bearer {token}"
                print("✅ Authenticated with Kalshi\n")
        except Exception as e:
            print(f"⚠️  Could not authenticate: {e}\n")

    # Fetch markets
    url = f"{base_url}/markets"
    params = {
        "limit": 100,
        "status": "open"  # Only open markets
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        markets = data.get("markets", [])

        print(f"Fetched {len(markets)} markets\n")

        # Look for markets with "2024" in title
        markets_2024 = [m for m in markets if "2024" in m.get("title", "")]

        if markets_2024:
            print(f"⚠️  FOUND {len(markets_2024)} MARKETS WITH '2024' IN TITLE:\n")

            for market in markets_2024[:5]:
                print("-" * 80)
                print(f"Title: {market.get('title', 'N/A')[:100]}")
                print(f"Status: {market.get('status')}")
                print(f"Open: {market.get('open_time')}")
                print(f"Close: {market.get('close_time')}")
                print(f"Expiration: {market.get('expiration_time')}")

                # Parse expiration time
                exp_time = market.get('expiration_time')
                if exp_time:
                    try:
                        exp_dt = datetime.fromisoformat(exp_time.replace("Z", "+00:00"))
                        now = datetime.now(timezone.utc)
                        is_past = exp_dt < now
                        print(f"  → Expiration parsed: {exp_dt}")
                        print(f"  → Is in the past? {is_past}")
                        print(f"  → Days until/since expiration: {(exp_dt - now).days}")
                    except Exception as e:
                        print(f"  → Error parsing expiration: {e}")

                print(f"\nVolume: ${market.get('volume', 0):,.0f}")
                print(f"ALL FIELDS: {list(market.keys())}")
                print()
        else:
            print("✅ No markets with '2024' in title found\n")

        # Show sample markets
        print("\n" + "-"*80)
        print("SAMPLE OF FIRST 3 MARKETS (regardless of year):\n")
        for i, market in enumerate(markets[:3], 1):
            print(f"{i}. {market.get('title', 'N/A')[:80]}")
            print(f"   Status: {market.get('status')} | Expiration: {market.get('expiration_time')}")
            print(f"   Volume: ${market.get('volume', 0):,.0f}\n")

    except Exception as e:
        print(f"❌ Error fetching Kalshi: {e}")


if __name__ == "__main__":
    print("\n🔍 TESTING MARKET DATE FIELDS FROM BOTH APIs")
    print("This will show why 2024 markets might still appear as 'active'\n")

    test_polymarket_dates()
    test_kalshi_dates()

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)
    print("\nLook for:")
    print("  • Markets with '2024' in title that have active=true")
    print("  • Date fields that are in the future despite being about past events")
    print("  • Missing or null date fields")
    print()
