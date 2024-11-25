import requests
from requests.adapters import HTTPAdapter
from typing import Optional, Tuple
from requests import Session
from requests.packages.urllib3.util.retry import Retry
from app import SessionLocal
from app import BetDescription, Price # Import BetDescription and Price tables from API
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
from datetime import datetime

# API endpoint
API_BASE_URL = "http://localhost:9000/api/v1/bets"

# Set up session with retry logic
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
session.mount("http://", HTTPAdapter(max_retries=retries))

# Load environment variables
load_dotenv()

#establish connection
def create_connection():
    connection = None
    try:
        #print(f"Attempting to connect to:")
        #print(f"Host: {os.getenv('DB_HOST')}")
        #print(f"User: {os.getenv('DB_USER')}")
        #print(f"Database: {os.getenv('DB_NAME')}")
        
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME')
        )
        print("Successfully connected to the database")
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        print(f"Error Code: {e.errno}")
        print(f"SQL State: {e.sqlstate}")
        print(f"Error Message: {e.msg}")
    return connection

def get_prices_by_bet_id(bet_id: int) -> Optional[Tuple[float, float]]:
    connection = create_connection()
    if connection is None:
        print("Failed to connect to the database.")
        return None, None
    
    query = """
    SELECT 
        p.yes_price, 
        p.no_price 
    FROM 
        price p
    JOIN 
        bet_choice bc ON p.option_id = bc.option_id
    WHERE 
        bc.bet_id = %s
    ORDER BY 
        p.timestamp DESC
    LIMIT 1
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (bet_id,))
            result = cursor.fetchone()
            
            if not result:
                print(f"No price data found for bet_id {bet_id}")
                return None, None
            
            price_yes, price_no = result
            print(f"Fetched prices for bet_id {bet_id}: price_yes = {price_yes}, price_no = {price_no}")
            
            return price_yes, price_no
    
    except mysql.connector.Error as e:
        print(f"Error fetching prices for bet_id {bet_id}: {e}")
        return None, None
    
    finally:
        connection.close()
        print("Database connection closed.")

# Update similar_event_ids to fetch from similar_events table in the database
def get_similar_event_ids():
    connection = create_connection()
    if connection is None:
        print("Failed to connect to the database.")
        return []
    
    query = """
    SELECT 
        bet_id_1, 
        bet_id_2
    FROM 
        similar_events
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            
            if not result:
                print("No similar events found.")
                return []
            
            similar_event_ids = [(row[0], row[1]) for row in result]
            print(f"Fetched similar event pairs: {similar_event_ids}")
            
            return similar_event_ids
    
    except mysql.connector.Error as e:
        print(f"Error fetching similar event pairs: {e}")
        return []
    
    finally:
        connection.close()
        print("Database connection closed.")

# Insert arbitrage opportunity into the database
def insert_arbitrage_opportunity(connection, bet_id_1: int, bet_id_2: int, profit: float):
    """
    Inserts an arbitrage opportunity into the arbitrage_opportunities table.
    """
    query = """
    INSERT INTO arbitrage_opportunities (bet_id1, bet_id2, timestamp, profit)
    VALUES (%s, %s, %s, %s)
    """
    timestamp = datetime.now()
    values = (bet_id_1, bet_id_2, timestamp, profit)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()
            print(f"Arbitrage opportunity added with ID: {cursor.lastrowid}")
    except Error as e:
        print(f"Error adding arbitrage opportunity: {e}")

ARBITRAGE_BASE_URL = "http://localhost:9000/api/v1/arbitrage"
def post_arbitrage_opportunity(connection, bet_id_1: int, bet_id_2: int, profit: float):
    """
    Temporarily skip API calls and insert directly into the database.
    """
    print(f"Skipping API call and inserting arbitrage opportunity for bet IDs {bet_id_1} and {bet_id_2} directly into the database.")
    insert_arbitrage_opportunity(connection, bet_id_1, bet_id_2, profit)

# Calculate cross-market arbitrage for a pair of bet IDs
def calculate_cross_market_arbitrage(bet_id_1, bet_id_2, connection):
    """
    Calculate arbitrage opportunities for a pair of bet IDs.
    Posts and inserts profitable opportunities into the database.
    """
    price_yes_market1, price_no_market1 = get_prices_by_bet_id(bet_id_1)
    price_yes_market2, price_no_market2 = get_prices_by_bet_id(bet_id_2)

    if None in [price_yes_market1, price_no_market1, price_yes_market2, price_no_market2]:
        print(f"Prices not available for bet IDs {bet_id_1} or {bet_id_2}. Skipping arbitrage calculation.")
        return

    # Fetch bet names
    bet_name_1 = get_bet_name_by_id(bet_id_1)
    bet_name_2 = get_bet_name_by_id(bet_id_2)

    # Scenario 1: "Yes" on Market 1, "No" on Market 2
    total_price_scenario_1 = price_yes_market1 + price_no_market2
    profit_scenario_1 = 100 - total_price_scenario_1 if total_price_scenario_1 < 100 else 0

    # Scenario 2: "No" on Market 1, "Yes" on Market 2
    total_price_scenario_2 = price_no_market1 + price_yes_market2
    profit_scenario_2 = 100 - total_price_scenario_2 if total_price_scenario_2 < 100 else 0

    if profit_scenario_1 > 0:
        print(f"Found arbitrage opportunity (Scenario 1): {profit_scenario_1} profit")
        print(f"  Bet 1: {bet_name_1} (ID: {bet_id_1}), Bet 2: {bet_name_2} (ID: {bet_id_2})")
        post_arbitrage_opportunity(connection, bet_id_1, bet_id_2, profit_scenario_1)

    if profit_scenario_2 > 0:
        print(f"Found arbitrage opportunity (Scenario 2): {profit_scenario_2} profit")
        print(f"  Bet 1: {bet_name_1} (ID: {bet_id_1}), Bet 2: {bet_name_2} (ID: {bet_id_2})")
        post_arbitrage_opportunity(connection, bet_id_2, bet_id_1, profit_scenario_2)

# Function to get bet_description from bet_id
def get_bet_name_by_id(bet_id: int) -> Optional[str]:
    """
    Fetches the name of the bet directly from the bet_description table.
    """
    connection = create_connection()
    if connection is None:
        print("Failed to connect to the database.")
        return None

    query = """
    SELECT name
    FROM bet_description
    WHERE bet_id = %s
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (bet_id,))
            result = cursor.fetchone()
            if result:
                return result[0]  # Return the name
            else:
                print(f"No bet name found for bet_id {bet_id}")
                return None
    except mysql.connector.Error as e:
        print(f"Error fetching bet name for bet_id {bet_id}: {e}")
        return None
    finally:
        if connection.is_connected():
            connection.close()


"""
# Main script
if __name__ == "__main__":
    connection = create_connection()  # Establish the database connection

    if connection is None:
        print("Failed to connect to the database. Exiting...")
        exit()

    arbitrage_results = []  # Initialize arbitrage_results here

    similar_event_ids = get_similar_event_ids()
    for bet_id_1, bet_id_2 in similar_event_ids:
        result = calculate_cross_market_arbitrage(bet_id_1, bet_id_2, connection)
        if result:
            arbitrage_results.append(result)

    # Display results
    for result in arbitrage_results:
        if result["arbitrage"]:
            print(f"Arbitrage opportunity for bets {result['bet_ids']}:")
            if result["scenario_1"]["profit_per_contract"] > 0:
                print("  Scenario 1: Bet on Yes in Market 1 and No in Market 2")
                print(f"    Total Investment: {result['scenario_1']['total_investment']} cents")
                print(f"    Profit per Contract: {result['scenario_1']['profit_per_contract']} cents")
            if result["scenario_2"]["profit_per_contract"] > 0:
                print("  Scenario 2: Bet on No in Market 1 and Yes in Market 2")
                print(f"    Total Investment: {result['scenario_2']['total_investment']} cents")
                print(f"    Profit per Contract: {result['scenario_2']['profit_per_contract']} cents")
        else:
            print(f"No arbitrage for bets {result['bet_ids']}. {result['message']}")

    connection.close()  # Close the database connection
"""

if __name__ == "__main__":
    connection = create_connection()  # Establish the database connection

    if connection is None:
        print("Failed to connect to the database. Exiting...")
        exit()

    arbitrage_results = []  # Initialize arbitrage_results here

    similar_event_ids = get_similar_event_ids()
    for bet_id_1, bet_id_2 in similar_event_ids:
        calculate_cross_market_arbitrage(bet_id_1, bet_id_2, connection)

    connection.close()  # Close the database connection

