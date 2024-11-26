import requests
from requests.adapters import HTTPAdapter
from typing import Optional, Tuple
from requests import Session
from requests.packages.urllib3.util.retry import Retry
from app import SessionLocal
from app import BetDescription, Price  
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

# Establish connection
def create_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME')
        )
        print("Successfully connected to the database")
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    return connection

# Unified function to fetch prices and adjust for Polymarket
def get_prices_by_bet_id(bet_id: int) -> Optional[Tuple[float, float]]:
    """
    Fetch prices for a given bet ID, and adjust if the bet is from Polymarket.
    """
    connection = create_connection()
    if connection is None:
        print("Failed to connect to the database.")
        return None, None

    query = """
    SELECT 
        p.yes_price, 
        p.no_price,
        bd.website
    FROM 
        price p
    JOIN 
        bet_choice bc ON p.option_id = bc.option_id
    JOIN 
        bet_description bd ON bc.bet_id = bd.bet_id
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
            
            raw_price_yes, raw_price_no, website = result
            print(f"Raw prices for bet_id {bet_id}: price_yes = {raw_price_yes}, price_no = {raw_price_no}, website = {website}")

            return raw_price_yes, raw_price_no
    
    except mysql.connector.Error as e:
        print(f"Error fetching prices for bet_id {bet_id}: {e}")
        return None, None
    
    finally:
        connection.close()
        print("Database connection closed.")

# Fetch similar event IDs
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

# Calculate cross-market arbitrage
def calculate_cross_market_arbitrage(bet_id_1, bet_id_2, website_1, website_2, connection):
    """
    Calculate arbitrage opportunities for a pair of bet IDs, considering the website.
    """
    # Fetch raw prices for both bet IDs
    price_yes_market1, price_no_market1 = get_prices_by_bet_id(bet_id_1)
    price_yes_market2, price_no_market2 = get_prices_by_bet_id(bet_id_2)

    if None in [price_yes_market1, price_no_market1, price_yes_market2, price_no_market2]:
        print(f"Prices not available for bet IDs {bet_id_1} or {bet_id_2}. Skipping arbitrage calculation.")
        return

    # Adjust prices based on website
    if website_1.lower() == "polymarket":
        price_yes_market1 *= 100
        price_no_market1 *= 100
    
    if website_2.lower() == "polymarket":
        price_yes_market2 *= 100
        price_no_market2 *= 100

    print(f"Adjusted prices for bet_id_1 ({bet_id_1}): price_yes = {price_yes_market1}, price_no = {price_no_market1}")
    print(f"Adjusted prices for bet_id_2 ({bet_id_2}): price_yes = {price_yes_market2}, price_no = {price_no_market2}")

    # Scenario 1: "Yes" on Market 1, "No" on Market 2
    total_price_scenario_1 = price_yes_market1 + price_no_market2
    profit_scenario_1 = 100 - total_price_scenario_1 if total_price_scenario_1 < 100 else 0

    # Scenario 2: "No" on Market 1, "Yes" on Market 2
    total_price_scenario_2 = price_no_market1 + price_yes_market2
    profit_scenario_2 = 100 - total_price_scenario_2 if total_price_scenario_2 < 100 else 0

    if profit_scenario_1 > 0:
        print(f"Found arbitrage opportunity (Scenario 1): {profit_scenario_1} profit")
        insert_arbitrage_opportunity(connection, bet_id_1, bet_id_2, profit_scenario_1)

    if profit_scenario_2 > 0:
        print(f"Found arbitrage opportunity (Scenario 2): {profit_scenario_2} profit")
        insert_arbitrage_opportunity(connection, bet_id_2, bet_id_1, profit_scenario_2)
   
# Fetch similar event IDs along with their websites
def get_similar_event_ids_with_websites():
    connection = create_connection()
    if connection is None:
        print("Failed to connect to the database.")
        return []
    
    query = """
    SELECT 
        bet_id_1, 
        website_1,
        bet_id_2,
        website_2
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
            
            similar_event_data = [(row[0], row[1], row[2], row[3]) for row in result]
            print(f"Fetched similar event pairs with websites: {similar_event_data}")
            return similar_event_data
    
    except mysql.connector.Error as e:
        print(f"Error fetching similar event pairs: {e}")
        return []
    
    finally:
        connection.close()
        print("Database connection closed.")

# Main script
if __name__ == "__main__":
    connection = create_connection()
    if connection is None:
        print("Failed to connect to the database. Exiting...")
        exit()

    # Fetch similar events with website information
    similar_event_data = get_similar_event_ids_with_websites()
    
    # Calculate arbitrage for each pair
    for bet_id_1, website_1, bet_id_2, website_2 in similar_event_data:
        calculate_cross_market_arbitrage(bet_id_1, bet_id_2, website_1, website_2, connection)

    connection.close()
