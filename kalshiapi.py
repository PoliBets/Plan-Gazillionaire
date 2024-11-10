import requests
import requests_cache
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from tqdm import tqdm
import main

def fetch_kalshi_events():
    session = requests_cache.CachedSession('requests_cache')
    cursor = ""
    events = []
    
    while True:
        url = "https://trading-api.kalshi.com/trade-api/v2/events?with_nested_markets=true&status=open"
        headers = {"accept": "application/json"}

        response = session.get(url, headers=headers, params={
            "limit": 200,
            "cursor": cursor,
            "with_nested_markets": True
        })

        r = response.json()
        
        if r.get("cursor") == cursor or not r.get("cursor"):
            break

        events.extend(r.get("events", []))
        cursor = r.get("cursor")

    return events

def insert_event_data(connection, events):
    bet_description_query = """
    INSERT INTO bet_description (bet_id, name, expiration_date, website, status, is_arbitrage)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    bet_choice_query = """
    INSERT INTO bet_choice (option_id, bet_id, name, outcome)
    VALUES (%s, %s, %s, %s)
    """
    
    price_query = """
    INSERT INTO price (option_id, timestamp, volume, yes_price, no_price, yes_odds, no_odds)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    

    bet_description_values = []
    bet_choice_values = []
    price_values = []
    
    option_id = 1 
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    
    for bet_id, event in enumerate(events, start=1):
        event_name = event.get("title")
        expiration_date = event.get("markets", [{}])[0].get("close_time")
        website = 'kalshi'
        status = 'open'
        is_arbitrage = 'no'
        
        bet_description_values.append((bet_id, event_name, expiration_date, website, status, is_arbitrage))
        
     
        for market in event.get("markets", []):
            market_subtitle = market.get("subtitle", "Unknown Choice")
            outcome = 'pending'
            
           
            bet_choice_values.append((option_id, bet_id, market_subtitle, outcome))
            
            
            yes_price = market.get("yes_bid", 0) / 100
            no_price = market.get("no_bid", 0) / 100
            yes_odds = 1 / yes_price if yes_price > 0 else 0
            no_odds = 1 / no_price if no_price > 0 else 0
            volume = market.get("volume", 0) 
            
            price_values.append((option_id, current_timestamp, volume, yes_price, no_price, yes_odds, no_odds))
            
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

if __name__ == "__main__":
    connection = main.create_connection()

    if connection:
        events = fetch_kalshi_events()
        insert_event_data(connection, events)
      
        connection.close()
        print("Database connection closed.")
    else:
        print("Failed to connect to the database.")
