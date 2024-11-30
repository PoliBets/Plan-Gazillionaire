import requests
import requests_cache
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from tqdm import tqdm
import hashlib
import main

def parse_date(date_str):
    formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None

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

def get_max_option_id(connection):
    """Get the maximum option_id currently in the database."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COALESCE(MAX(option_id), 0) FROM bet_choice")
            result = cursor.fetchone()
            return result[0] if result else 0
    except Error as e:
        print(f"Error fetching max option_id: {e}")
        return 0

def clear_kalshi_events(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS=0;")

            cursor.execute("SELECT bet_id FROM bet_description WHERE website = 'kalshi'")
            kalshi_bet_ids = [row[0] for row in cursor.fetchall()]
            print(f"Found {len(kalshi_bet_ids)} bet_ids to delete: {kalshi_bet_ids}")

            if kalshi_bet_ids:
                cursor.execute(
                    "SELECT option_id FROM bet_choice WHERE bet_id IN (%s)" %
                    ", ".join(map(str, kalshi_bet_ids))
                )
                kalshi_option_ids = [row[0] for row in cursor.fetchall()]
                print(f"Found {len(kalshi_option_ids)} option_ids to delete: {kalshi_option_ids}")

                if kalshi_option_ids:
                    cursor.execute(
                        "DELETE FROM price WHERE option_id IN (%s)" %
                        ", ".join(map(str, kalshi_option_ids))
                    )
                    print(f"Deleted {cursor.rowcount} rows from price.")

                cursor.execute(
                    "DELETE FROM bet_choice WHERE bet_id IN (%s)" %
                    ", ".join(map(str, kalshi_bet_ids))
                )
                print(f"Deleted {cursor.rowcount} rows from bet_choice.")

            cursor.execute("DELETE FROM bet_description WHERE website = 'kalshi'")
            print(f"Deleted {cursor.rowcount} rows from bet_description.")

            cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
            connection.commit()
            print("Successfully cleared old Kalshi data.")
    except Error as e:
        print(f"Error clearing Kalshi events: {e}")



def insert_event_data(connection, events):
    bet_description_query = """
    INSERT INTO bet_description (bet_id, name, expiration_date, website, status, is_arbitrage)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        name=VALUES(name),
        expiration_date=VALUES(expiration_date),
        website=VALUES(website),
        status=VALUES(status),
        is_arbitrage=VALUES(is_arbitrage)
    """

    bet_choice_query = """
    INSERT INTO bet_choice (option_id, bet_id, name, outcome)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        name=VALUES(name),
        outcome=VALUES(outcome)
    """

    price_query = """
    INSERT INTO price (option_id, timestamp, volume, yes_price, no_price, yes_odds, no_odds)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        timestamp=VALUES(timestamp),
        volume=VALUES(volume),
        yes_price=VALUES(yes_price),
        no_price=VALUES(no_price),
        yes_odds=VALUES(yes_odds),
        no_odds=VALUES(no_odds)
    """

    bet_description_values = []
    bet_choice_values = []
    price_values = []

    option_id = get_max_option_id(connection) + 1

    print("Inserting event data into the database...")
    for event in events:
        event_name = event.get("title")
        expiration_date = parse_date(event["markets"][0]["close_time"]) if event.get("markets") else None
        bet_id = int(hashlib.md5(f"{event_name}-{expiration_date}".encode()).hexdigest(), 16) % (10**8)

        bet_description_values.append((bet_id, event_name, expiration_date, "kalshi", "open", "no"))

        for market in event.get("markets", []):
            market_subtitle = market.get("subtitle", "Unknown Choice")
            volume = market.get("volume", 0)
            yes_price = market.get("yes_bid", 0)
            no_price = market.get("no_bid", 0)

            bet_choice_values.append((option_id, bet_id, market_subtitle, "pending"))
            price_values.append((option_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), volume, yes_price, no_price, yes_price, no_price))

            option_id += 1

    try:
        with connection.cursor() as cursor:
            cursor.executemany(bet_description_query, bet_description_values)
            cursor.executemany(bet_choice_query, bet_choice_values)
            cursor.executemany(price_query, price_values)
            connection.commit()
            print("Inserted all event data successfully.")
    except Error as e:
        print(f"Error inserting event data: {e}")

def get_kalshi_info():
    connection = main.create_connection()

    if connection:
        clear_kalshi_events(connection)
        events = fetch_kalshi_events()
        insert_event_data(connection, events)
        connection.close()
    else:
        print("Failed to connect to the database.")


