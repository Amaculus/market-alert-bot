#!/usr/bin/env python3
"""Debug script to inspect digest queue"""
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models import DigestQueue, AlertLog, SessionLocal

def inspect_queue():
    session = SessionLocal()
    try:
        # Get all queued items (both sent and unsent)
        all_items = session.query(DigestQueue)\
            .order_by(DigestQueue.queued_at.desc())\
            .limit(20)\
            .all()

        print(f"\n=== DIGEST QUEUE (Last 20 items) ===\n")

        for item in all_items:
            status = "SENT" if item.included_in_digest else "QUEUED"
            print(f"[{status}] Market: {item.market_title[:60]}")
            print(f"  ID: {item.market_id}")
            print(f"  Platform: {item.platform}")
            print(f"  Queued: {item.queued_at}")
            if item.included_in_digest:
                print(f"  Sent: {item.digest_sent_at}")

            # Check raw_data
            print(f"  Raw data present: {bool(item.raw_data)}")
            if item.raw_data:
                try:
                    raw = json.loads(item.raw_data)
                    print(f"  Raw data size: {len(item.raw_data)} bytes")
                    print(f"  Raw data keys: {list(raw.keys())}")

                    # Check URL components
                    if item.platform == 'polymarket':
                        slug = raw.get('slug')
                        cid = raw.get('condition_id')
                        question = raw.get('question', '')[:50]

                        print(f"  Question: {question}")
                        print(f"  Slug: {slug}")
                        print(f"  Condition_id: {cid}")

                        if slug:
                            url = f"https://polymarket.com/event/{slug}"
                            print(f"  Generated URL: {url}")
                        elif cid:
                            url = f"https://polymarket.com/event/{cid}"
                            print(f"  Generated URL (using cid): {url}")
                        else:
                            print(f"  ERROR: No slug or condition_id found!")

                    elif item.platform == 'kalshi':
                        ticker = raw.get('ticker')
                        title = raw.get('title', '')[:50]

                        print(f"  Title: {title}")
                        print(f"  Ticker: {ticker}")

                        if ticker:
                            url = f"https://kalshi.com/markets/{ticker.lower()}"
                            print(f"  Generated URL: {url}")
                        else:
                            print(f"  ERROR: No ticker found!")

                except Exception as e:
                    print(f"  ERROR parsing raw_data: {e}")
                    print(f"  Raw data (first 200 chars): {item.raw_data[:200]}")
            else:
                print(f"  WARNING: NO RAW DATA - links will be broken!")

            print("-" * 80)

    finally:
        session.close()

if __name__ == '__main__':
    inspect_queue()
