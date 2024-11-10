import requests
import ast
import main
from datetime import datetime
from mysql.connector import Error
#gets the information from polybets api
#formats information into a list for database entry

base_url = "https://gamma-api.polymarket.com/events"
params = {
    "closed": "false",
    "limit": 100,
    "offset": 0
}

political_events = {}
count = 0

connection = main.create_connection()

while True:
    r = requests.get(base_url, params=params)
    response = r.json()

    if not response:
        break

    for event in response:
        count += 1 #increase count to see how many responses
        list_tags = [tag['slug'] for tag in event['tags']]
    
        if any("politics" in tag for tag in list_tags): #determines if the event is tagged politics
            info_list = []
            markets = {}
        
        #add info
            political_events[event['id']] = info_list
            info_list.append(event['title'])
            if 'endDate' in event:
                end_date = event['endDate'].split('T')
                info_list.append(end_date[0]) 
            info_list.append('polymarket') 
            info_list.append('open') 
        
            for market in event['markets']:
                markets['key'] = market['id']
                markets['question'] = market['question']
                if 'volume' in market:
                    markets['volume'] = market['volume']
                
                clean_outcomes = ast.literal_eval(market['outcomes'])
                if 'outcomePrices' in market:
                    clean_outcomePrices = ast.literal_eval(market['outcomePrices'])
            
                i = 0
                while i < len(clean_outcomes):
                    key = f'outcome {i+1}' 
                    key2 = f'outcome {i+1} price'
                    markets[key] = clean_outcomes[i]
                    markets[key2] = clean_outcomePrices[i]
                    i = i + 1

                info_list.append(markets)
    params["offset"] += 100

print(political_events) #check to make sure dict is correct
print(count) #check number of results

for event_id, info_list in political_events.items():

    #insert info into bet_description table
    if not main.bet_exists(connection, event_id):
        insert_query = """
        INSERT INTO bet_description (bet_id, name, expiration_date, website, status, is_arbitrage)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (event_id, info_list[0], info_list[1], "polymarket", "open", "no")
        
        try:
            with connection.cursor() as cursor:
                cursor.execute(insert_query, values)
                connection.commit()
                print("entered bet description successfully")
        except Error as e:
            print(f"Error inserting bet descriptions: {e}")
    else:
        update_query = """
        UPDATE bet_description
        SET name = %s, expiration_date = %s, website = %s, status = %s, is_arbitrage = %s
        WHERE bet_id = %s
        """
        update_values = (info_list[0], info_list[1], "polymarket", "open", "no", event_id)
        
        try:
            with connection.cursor() as cursor:
                cursor.execute(update_query, update_values)
                connection.commit()
                print("updated bet description successfully")
        except Error as e:
            print(f"Error updating bet descriptions: {e}")
    
    #insert into bet_choices table
    markets = info_list[4]
    if markets:
        if not main.option_exists(connection, markets['key']):
            insert_query = """
            INSERT INTO bet_choice (option_id, bet_id, name, outcome)
            VALUES (%s, %s, %s, %s)
            """
            values = (markets['key'], event_id, markets['question'], "pending")
        
            try:
                with connection.cursor() as cursor:
                    cursor.execute(insert_query, values)
                    connection.commit()
                    print("entered bet choice successfully")
            except Error as e:
                print(f"Error inserting bet descriptions: {e}")
        else:
            update_query = """
            UPDATE bet_choice
            SET name = %s, outcome = %s
            WHERE option_id = %s
            """
            update_values = (markets['question'], "pending", markets['key'])
        
            try:
                with connection.cursor() as cursor:
                    cursor.execute(update_query, update_values)
                    connection.commit()
                    print("updated bet choice successfully")
            except Error as e:
                print(f"Error updating bet descriptions: {e}")
        
        #enters prices into price table
        i = 1
        while f'outcome {i}' in markets:
            if not main.price_exists(connection, markets['key']):
                insert_query = """
                INSERT INTO price (option_id, timestamp, volume, yes_price, no_price, yes_odds, no_odds)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                values = (markets['key'], datetime.now().strftime('%Y-%m-%d %H:%M:%S'), markets['volume'], markets['outcome 1 price'], markets['outcome 2 price'], markets['outcome 1 price'], markets['outcome 2 price'])
        
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(insert_query, values)
                        connection.commit()
                        print("entered price successfully")
                except Error as e:
                    print(f"Error inserting bet descriptions: {e}")
            else:
                update_query = """
                UPDATE price
                SET timestamp = %s,
                    volume = %s,
                    yes_price = %s,
                    no_price = %s,
                    yes_odds = %s,
                    no_odds = %s
                WHERE option_id = %s
                """
                values = (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), markets['volume'], markets['outcome 1 price'], markets['outcome 2 price'], markets['outcome 1 price'], markets['outcome 2 price'], markets['key'])
    
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(update_query, values)
                        connection.commit()
                        print("entered price successfully")
                except Error as e:
                    print(f"Error updating price record: {e}")
            i += 1
print("Done")
main.connection.close()
        