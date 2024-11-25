import requests
import ast
import main
from datetime import datetime
from mysql.connector import Error
import threading

# Function to process each response and add the data to shared lists
def process_response(response, political_events, bet_choices, prices, lock):
    # Create a new database connection for each thread
    connection = main.create_connection()

    for event in response:
        list_tags = [tag['slug'] for tag in event['tags']]

        if any("politics" in tag for tag in list_tags):
            bet_id = event['id']
            title = event['title']
            if 'endDate' in event:
                end_date = event['endDate'].split('T')
                expiration_date = end_date[0]
            if not main.bet_exists(connection, bet_id):
                with lock:
                    political_events.append((bet_id, title, expiration_date, "polymarket", "open", "no"))
                print("political event added")
            else:
                print("no new political event")

            for market in event['markets']:
                market_id = market['id']
                question = market['question']
                if 'volume' in market:
                    volume = market['volume']
                
                if not main.option_exists(connection, market_id):
                    with lock:
                        bet_choices.append((market_id, bet_id, question, "pending"))
                    print("market added")
                else:
                    print("no new market")

                clean_outcomes = ast.literal_eval(market['outcomes'])
                if 'outcomePrices' in market:
                    clean_outcomePrices = ast.literal_eval(market['outcomePrices'])
            
                if not main.price_exists(connection, market_id) and clean_outcomePrices:
                    with lock:
                        prices.append((market_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), volume, clean_outcomePrices[0]*100, clean_outcomePrices[1]*100, clean_outcomePrices[0]*100, clean_outcomePrices[1]*100))
                    print("price added")
                else:
                    print("no new price")

    # Close the connection when the work is done
    connection.close()


def getpolymarketinfo():
    base_url = "https://gamma-api.polymarket.com/events"
    params = {
        "closed": "false",
        "limit": 100,
        "offset": 0
    }

    political_events = []
    bet_choices = []
    prices = []

    lock = threading.Lock()
    threads = []

    while True:
        r = requests.get(base_url, params=params)
        response = r.json()

        if not response:
            break

        # Start a thread to process this entire response
        thread = threading.Thread(target=process_response, args=(response, political_events, bet_choices, prices, lock))
        thread.start()
        threads.append(thread)

        params["offset"] += 100

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    # Insert info into the database after all threads have finished
    connection = main.create_connection()

    if political_events:
        insert_query = """
            INSERT INTO bet_description (bet_id, name, expiration_date, website, status, is_arbitrage)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE name=VALUES(name), expiration_date=VALUES(expiration_date),
                                    website=VALUES(website), status=VALUES(status), is_arbitrage=VALUES(is_arbitrage)
        """
        with connection.cursor() as cursor:
            cursor.executemany(insert_query, political_events)
        connection.commit()

    if bet_choices:
        insert_query = """
            INSERT INTO bet_choice (option_id, bet_id, name, outcome)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE name=VALUES(name), outcome=VALUES(outcome)
        """
        with connection.cursor() as cursor:
            cursor.executemany(insert_query, bet_choices)
        connection.commit()

    if prices:
        insert_query = """
            INSERT INTO price (option_id, timestamp, volume, yes_price, no_price, yes_odds, no_odds)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE timestamp=VALUES(timestamp), volume=VALUES(volume),
                                    yes_price=VALUES(yes_price), no_price=VALUES(no_price),
                                    yes_odds=VALUES(yes_odds), no_odds=VALUES(no_odds)
        """
        with connection.cursor() as cursor:
            cursor.executemany(insert_query, prices)
        connection.commit()
    
    connection.close()
    print(f"Inserted {len(political_events)} bet descriptions, {len(bet_choices)} bet choices, and {len(prices)} prices successfully.")


getpolymarketinfo()
