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
            print(f"[WARNING]  FOUND {len(markets_2024)} MARKETS WITH '2024' IN TITLE:\n")

            for market in markets_2024[:5]:  # Show first 5
                print("-" * 80)
                print(f"Question: {market.get('question', 'N/A')[:100]}")
                print(f"Active: {market.get('active')}")
                print(f"Closed: {market.get('closed')}")
                print(f"Archived: {market.get('archived')}")

                # ALL Polymarket date fields
                date_fields = {
                    'startDate': market.get('startDate'),
                    'endDate': market.get('endDate'),
                    'startDateIso': market.get('startDateIso'),
                    'endDateIso': market.get('endDateIso'),
                    'umaEndDate': market.get('umaEndDate'),
                    'umaEndDateIso': market.get('umaEndDateIso'),
                    'closedTime': market.get('closedTime'),
                    'gameStartTime': market.get('gameStartTime'),
                    'eventStartTime': market.get('eventStartTime'),
                }

                print(f"\nALL DATE FIELDS:")
                now = datetime.now(timezone.utc)

                for field_name, field_value in date_fields.items():
                    if field_value:
                        print(f"\n  {field_name}: {field_value}")
                        # Try to parse
                        try:
                            if isinstance(field_value, str):
                                dt = datetime.fromisoformat(field_value.replace("Z", "+00:00"))
                            else:
                                # Might be a timestamp
                                dt = datetime.fromtimestamp(field_value, tz=timezone.utc)

                            is_past = dt < now
                            days_diff = (dt - now).days
                            print(f"    → Parsed: {dt}")
                            print(f"    → Is PAST? {is_past}")
                            print(f"    → Days until/since: {days_diff}")
                        except Exception as e:
                            print(f"    → Could not parse: {e}")
                    else:
                        print(f"  {field_name}: None")

                # Volume info
                print(f"\nVOLUME: ${market.get('volume', 0):,.0f}")
                print()
        else:
            print("[OK] No markets with '2024' in title found in first 20\n")

        # Show a few sample markets regardless
        print("\n" + "-"*80)
        print("SAMPLE OF FIRST 3 MARKETS (regardless of year):\n")
        for i, market in enumerate(markets[:3], 1):
            print(f"{i}. {market.get('question', 'N/A')[:80]}")
            print(f"   Active: {market.get('active')} | end_date_iso: {market.get('end_date_iso')}")
            print(f"   Volume: ${market.get('volume', 0):,.0f}\n")

    except Exception as e:
        print(f"[ERROR] Error fetching Polymarket: {e}")


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
                print("[OK] Authenticated with Kalshi\n")
        except Exception as e:
            print(f"[WARNING]  Could not authenticate: {e}\n")

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
            print(f"[WARNING]  FOUND {len(markets_2024)} MARKETS WITH '2024' IN TITLE:\n")

            for market in markets_2024[:5]:
                print("-" * 80)
                print(f"Title: {market.get('title', 'N/A')[:100]}")
                print(f"Status: {market.get('status')}")

                # ALL Kalshi date fields
                date_fields = {
                    'close_time': market.get('close_time'),
                    'expiration_time': market.get('expiration_time'),
                    'latest_expiration_time': market.get('latest_expiration_time'),
                    'settlement_ts': market.get('settlement_ts'),
                    'expected_expiration_time': market.get('expected_expiration_time'),
                }

                print(f"\nALL DATE FIELDS:")
                now = datetime.now(timezone.utc)

                for field_name, field_value in date_fields.items():
                    if field_value:
                        print(f"\n  {field_name}: {field_value}")
                        # Try to parse
                        try:
                            if isinstance(field_value, str):
                                dt = datetime.fromisoformat(field_value.replace("Z", "+00:00"))
                            else:
                                # Might be a timestamp
                                dt = datetime.fromtimestamp(field_value, tz=timezone.utc)

                            is_past = dt < now
                            days_diff = (dt - now).days
                            print(f"    → Parsed: {dt}")
                            print(f"    → Is PAST? {is_past}")
                            print(f"    → Days until/since: {days_diff}")
                        except Exception as e:
                            print(f"    → Could not parse: {e}")
                    else:
                        print(f"  {field_name}: None")

                print(f"\nVOLUME: ${market.get('volume', 0):,.0f}")
                print()
        else:
            print("[OK] No markets with '2024' in title found\n")

        # Show sample markets
        print("\n" + "-"*80)
        print("SAMPLE OF FIRST 3 MARKETS (regardless of year):\n")
        for i, market in enumerate(markets[:3], 1):
            print(f"{i}. {market.get('title', 'N/A')[:80]}")
            print(f"   Status: {market.get('status')} | Expiration: {market.get('expiration_time')}")
            print(f"   Volume: ${market.get('volume', 0):,.0f}\n")

    except Exception as e:
        print(f"[ERROR] Error fetching Kalshi: {e}")


def test_api_filtering():
    """Test if we can filter markets at API level using date parameters"""
    print("\n" + "="*80)
    print("TESTING API-LEVEL DATE FILTERING")
    print("="*80 + "\n")

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    # Test Polymarket with end_date_min filter
    print("Testing Polymarket with end_date_min (only future markets):")
    url = "https://gamma-api.polymarket.com/markets"
    params = {
        "limit": 20,
        "end_date_min": now_iso,  # Only markets ending in the future
        "archived": "false"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        markets = response.json()
        print(f"  [OK] Got {len(markets)} markets with end_date_min={now_iso[:10]}")

        # Check if any have 2024 in title
        markets_2024 = [m for m in markets if "2024" in m.get("question", "")]
        if markets_2024:
            print(f"  [WARNING]  Still found {len(markets_2024)} markets with '2024' in title!")
        else:
            print(f"  [OK] No 2024 markets found when using end_date_min filter")
    except Exception as e:
        print(f"  [ERROR] Error: {e}")

    print()


if __name__ == "__main__":
    print("\n*** TESTING MARKET DATE FIELDS FROM BOTH APIs ***")
    print("This will show why 2024 markets might still appear as 'active'\n")

    test_polymarket_dates()
    test_kalshi_dates()
    test_api_filtering()

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)
    print("\nLook for:")
    print("  • Markets with '2024' in title that have active=true")
    print("  • Date fields that are in the future despite being about past events")
    print("  • Missing or null date fields")
    print("  • Whether API-level filtering works to exclude past markets")
    print()
