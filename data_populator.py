from mysql.connector import Error

def insert_bet_descriptions(connection):
    query = """
    INSERT INTO bet_description (bet_id, name, expiration_date, website, event_type, status, is_arbitrage)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    values = [
        (1, 'US Presidential Election 2024', '2024-11-05', 'polymarket', 'election', 'open', 'yes'),
        (2, 'Federal Interest Rate Hike by December', '2024-12-15', 'kalshi', 'fed', 'open', 'no'),
        (3, 'Bitcoin price to hit $50k by 2024', '2024-12-31', 'polymarket', 'crypto', 'open', 'no'),
        (4, 'US Inflation to exceed 5% in Q4', '2024-12-31', 'kalshi', 'financial', 'open', 'yes')
    ]
    
    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, values)
            connection.commit()
            print("Inserted bet descriptions successfully.")
    except Error as e:
        print(f"Error inserting bet descriptions: {e}")

def insert_bet_choices(connection):
    query = """
    INSERT INTO bet_choice (option_id, bet_id, name, outcome)
    VALUES (%s, %s, %s, %s)
    """
    values = [
        (1, 1, 'Biden Wins', 'pending'),
        (2, 1, 'Trump Wins', 'pending'),
        (3, 2, 'Rate Hike', 'pending'),
        (4, 2, 'No Rate Hike', 'pending'),
        (5, 3, 'BTC > $50k', 'pending'),
        (6, 3, 'BTC < $50k', 'pending'),
        (7, 4, 'Inflation > 5%', 'pending'),
        (8, 4, 'Inflation ≤ 5%', 'pending')
    ]
    
    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, values)
            connection.commit()
            print("Inserted bet choices successfully.")
    except Error as e:
        print(f"Error inserting bet choices: {e}")

def insert_prices(connection):
    query = """
    INSERT INTO price (option_id, timestamp, volume, yes_price, no_price, yes_odds, no_odds)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    values = [
        (1, '2024-10-08 10:00:00', 200.50, 0.65, 0.35, 1.54, 2.86),
        (2, '2024-10-08 10:00:00', 150.75, 0.45, 0.55, 2.22, 1.82),
        (3, '2024-10-08 10:30:00', 300.25, 0.70, 0.30, 1.43, 3.33),
        (4, '2024-10-08 10:30:00', 100.00, 0.40, 0.60, 2.50, 1.67),
        (5, '2024-10-08 11:00:00', 500.00, 0.80, 0.20, 1.25, 5.00),
        (6, '2024-10-08 11:00:00', 450.00, 0.20, 0.80, 5.00, 1.25),
        (7, '2024-10-08 11:30:00', 120.00, 0.55, 0.45, 1.82, 2.22),
        (8, '2024-10-08 11:30:00', 130.00, 0.45, 0.55, 2.22, 1.82)
    ]
    
    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, values)
            connection.commit()
            print("Inserted prices successfully.")
    except Error as e:
        print(f"Error inserting prices: {e}")

def insert_arbitrage_opportunities(connection):
    query = """
    INSERT INTO arbitrage_opportunities (arb_id, bet_id1, bet_id2, timestamp, profit)
    VALUES (%s, %s, %s, %s, %s)
    """
    values = [
        (1, 1, 4, '2024-10-08 12:00:00', 15.50)
    ]
    
    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, values)
            connection.commit()
            print("Inserted arbitrage opportunities successfully.")
    except Error as e:
        print(f"Error inserting arbitrage opportunities: {e}")

def populate_database(connection):
    insert_bet_descriptions(connection)
    insert_bet_choices(connection)
    insert_prices(connection)
    insert_arbitrage_opportunities(connection)
