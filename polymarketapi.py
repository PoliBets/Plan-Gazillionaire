import requests
import ast
import main
from datetime import datetime
from mysql.connector import Error
#gets the information from polybets api
#formats information into a list for database entry

def getpolymarketinfo():
    base_url = "https://gamma-api.polymarket.com/events"
    params = {
        "closed": "false",
        "limit": 100,
        "offset": 0
    }

    connection = main.create_connection()

    political_events = []
    bet_choices = []
    prices = []

    while True:
        r = requests.get(base_url, params=params)
        response = r.json()

        if not response:
            break

        for event in response:
            list_tags = [tag['slug'] for tag in event['tags']]
    
            if any("politics" in tag for tag in list_tags): #determines if the event is tagged politics
        
                #add info
                bet_id = event['id']
                title = event['title']
                if 'endDate' in event:
                    end_date = event['endDate'].split('T')
                    expiration_date = end_date[0]
                if not main.bet_exists(connection, bet_id):
                    political_events.append((bet_id, title, expiration_date, "polymarket", "open", "no"))
                    print("political events done")

                for market in event['markets']:
                    market_id = market['id']
                    question = market['question']
                    if 'volume' in market:
                        volume = market['volume']
                    
                    if not main.option_exists(connection, market_id):
                        bet_choices.append((market_id, bet_id, question, "pending"))
                        print("markets done")

                    clean_outcomes = ast.literal_eval(market['outcomes'])
                    if 'outcomePrices' in market:
                        clean_outcomePrices = ast.literal_eval(market['outcomePrices'])
            
                    if clean_outcomePrices:
                        prices.append((market_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), volume, clean_outcomePrices[0], clean_outcomePrices[1], clean_outcomePrices[0], clean_outcomePrices[1]))
                        print("prices done")

        params["offset"] += 100

    #insert info into bet_description table
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
    
        #insert into bet_choices table
    if bet_choices:
        insert_query = """
            INSERT INTO bet_choice (option_id, bet_id, name, outcome)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE name=VALUES(name), outcome=VALUES(outcome)
        """
        with connection.cursor() as cursor:
            cursor.executemany(insert_query, bet_choices)
        connection.commit()

        #enters prices into price table

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

        