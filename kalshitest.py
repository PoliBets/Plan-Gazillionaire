import requests
import requests_cache
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from tqdm import tqdm
import hashlib
import main

def fetch_kalshi_events():
    session = requests_cache.CachedSession('requests_cache')
    limit = 200
    events = []
    cursor = None

    print("Fetching events from Kalshi API...")
    url = "https://api.elections.kalshi.com/trade-api/v2/events"
    headers = {"accept": "application/json"}

    while True:
        params = {"limit": limit, "with_nested_markets": True, "status": "open"}
        if cursor:
            params["cursor"] = cursor

        response = session.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Failed to fetch data: {response.status_code} - {response.text}")
            break

        r = response.json()
        batch = r.get("events", [])
        political_events = [event for event in batch if event.get("category") in ["Politics", "World", "Economics"]]
        events.extend(political_events)

        print(f"Fetched {len(political_events)} political events (Total: {len(events)})")
        cursor = r.get("cursor")
        if not cursor:
            break

    print(f"Total political events fetched: {len(events)}")
    return events

events = fetch_kalshi_events()

for event in events:
    print(f"Event Title: {event.get("title")}")
    for market in event.get('markets'):
        print(f"title: {market.get('title')}")
        print(f"subtitle: {market.get('subtitle')}")
        print(market.get('subtitle'))
        print(f"yes_sub_title: {market.get('yes_sub_title')}")
        print(f"no_sub_title: {market.get('no_sub_title')}")
        print("End Market")
        print("")
    print("End Event")
    print("")