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
    print(f"Invalid date format: {date_str}")
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
        params = {
            "limit": limit,
            "with_nested_markets": True,
            "status": "open"
        }
        if cursor:
            params["cursor"] = cursor

        print(f"Requesting data with params: {params}")
        response = session.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Failed to fetch data: {response.status_code} - {response.text}")
            break

        r = response.json()
        batch = r.get("events", [])
        
        # Filter only Political events
        political_events = [event for event in batch if event.get("category") in ["Politics", "World", "Economics"]]
        events.extend(political_events)

        print(f"Fetched {len(political_events)} political events in this batch (Total: {len(events)})")

        cursor = r.get("cursor")
        if not cursor:
            print("No more data to fetch. Pagination complete.")
            break

    print(f"Total political events fetched: {len(events)}")
    return events

def generate_unique_bet_id(event_name, expiration_date):
    """Generate a unique bet_id based on event_name and expiration_date."""
    raw_string = f"{event_name}-{expiration_date}"
    return int(hashlib.md5(raw_string.encode()).hexdigest(), 16) % (10**8)

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
            # Step 1: Delete rows in similar_options (both option_id_1 and option_id_2)
            cursor.execute("""
                DELETE similar_options
                FROM similar_options
                WHERE option_id_1 IN (
                    SELECT option_id
                    FROM bet_choice
                    WHERE bet_id IN (
                        SELECT bet_id
                        FROM bet_description
                        WHERE website = 'kalshi'
                    )
                ) OR option_id_2 IN (
                    SELECT option_id
                    FROM bet_choice
                    WHERE bet_id IN (
                        SELECT bet_id
                        FROM bet_description
                        WHERE website = 'kalshi'
                    )
                )
            """)
            print(f"Deleted {cursor.rowcount} rows from similar_options.")

            # Step 2: Delete rows in price
            cursor.execute("""
                DELETE price
                FROM price
                WHERE option_id IN (
                    SELECT option_id
                    FROM bet_choice
                    WHERE bet_id IN (
                        SELECT bet_id
                        FROM bet_description
                        WHERE website = 'kalshi'
                    )
                )
            """)
            print(f"Deleted {cursor.rowcount} rows from price.")

            # Step 3: Delete rows in bet_choice
            cursor.execute("""
                DELETE bet_choice
                FROM bet_choice
                WHERE bet_id IN (
                    SELECT bet_id
                    FROM bet_description
                    WHERE website = 'kalshi'
                )
            """)
            print(f"Deleted {cursor.rowcount} rows from bet_choice.")

            # Step 4: Delete rows in bet_description
            cursor.execute("DELETE FROM bet_description WHERE website = 'kalshi'")
            print(f"Deleted {cursor.rowcount} rows from bet_description.")

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
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print("Inserting event data into the database...")
    with tqdm(total=len(events), desc="Inserting events", unit="event") as pbar:
        for event in events:
            event_name = event.get("title")
            raw_expiration_date = event.get("markets", [{}])[0].get("close_time")
            expiration_date = parse_date(raw_expiration_date) if raw_expiration_date else None

            if not event_name or not expiration_date:
                print(f"Skipping event due to missing name or expiration_date: {event}")
                continue

            bet_id = generate_unique_bet_id(event_name, expiration_date)

            website = 'kalshi'
            status = 'open'
            is_arbitrage = 'no'

            bet_description_values.append((bet_id, event_name, expiration_date, website, status, is_arbitrage))
            
            seen_options = set()

            for market in event.get("markets", []):
                market_subtitle = market.get("subtitle", "Unknown Choice")
                outcome = 'pending'

                # Skip duplicate options for the same bet_id
                if (bet_id, market_subtitle) in seen_options:
                    continue
                seen_options.add((bet_id, market_subtitle))
                
                bet_choice_values.append((option_id, bet_id, market_subtitle, outcome))
                
                yes_price = market.get("yes_bid", 0)
                no_price = market.get("no_bid", 0)
                yes_odds = market.get("yes_bid", 0)
                no_odds = market.get("no_bid", 0)
                volume = market.get("volume", 0)
                
                price_values.append((option_id, current_timestamp, volume, yes_price, no_price, yes_odds, no_odds))
                
                option_id += 1

            pbar.update(1)
            
    try:
        with connection.cursor() as cursor:
            if bet_description_values:
                cursor.executemany(bet_description_query, bet_description_values)
            if bet_choice_values:
                cursor.executemany(bet_choice_query, bet_choice_values)
            if price_values:
                cursor.executemany(price_query, price_values)
            connection.commit()
            print("Inserted all event data successfully.")
    except Error as e:
        print(f"Error inserting event data: {e}")


if __name__ == "__main__":
    connection = main.create_connection()

    if connection:
        clear_kalshi_events(connection)  # Clear old Kalshi data
        events = fetch_kalshi_events()
        insert_event_data(connection, events)
        connection.close()
        print("Database connection closed.")
    else:
        print("Failed to connect to the database.")
