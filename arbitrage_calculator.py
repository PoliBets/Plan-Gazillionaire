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
def calculate_cross_market_arbitrage(bet_id_1, bet_id_2, event_1_details, event_2_details, connection):
    """
    Calculate arbitrage opportunities for a pair of bet IDs, considering the website.
    """
    # Fetch event details for both bets
    event_1_details = get_bet_description(bet_id_1, connection)
    event_2_details = get_bet_description(bet_id_2, connection)

    if not event_1_details or not event_2_details:
        print(f"Missing details for bets {bet_id_1} or {bet_id_2}. Skipping arbitrage calculation.")
        return

    # Ensure the events are on different platforms
    if event_1_details["website"] == event_2_details["website"]:
        print(f"Skipping arbitrage calculation: Both bets are on the same platform ({event_1_details['website']}).")
        return
    
    # Ensure the events are on different platforms
    if event_1_details["website"] == event_2_details["website"]:
        print(f"Skipping arbitrage calculation: Both bets are on the same platform ({event_1_details['website']}).")
        return

    # Fetch raw prices for both bet IDs
    price_yes_market1, price_no_market1 = get_prices_by_bet_id(bet_id_1)
    price_yes_market2, price_no_market2 = get_prices_by_bet_id(bet_id_2)

    if None in [price_yes_market1, price_no_market1, price_yes_market2, price_no_market2]:
        print(f"Prices not available for bet IDs {bet_id_1} or {bet_id_2}. Skipping arbitrage calculation.")
        return

    # Adjust prices based on website
    if event_1_details["website"].lower() == "polymarket":
        price_yes_market1 *= 100
        price_no_market1 *= 100

    if event_2_details["website"].lower() == "polymarket":
        price_yes_market2 *= 100
        price_no_market2 *= 100

    print(f"Adjusted prices for bet_id_1 ({bet_id_1}, {event_1_details}): price_yes = {price_yes_market1}, price_no = {price_no_market1}")
    print(f"Adjusted prices for bet_id_2 ({bet_id_2}, {event_2_details}): price_yes = {price_yes_market2}, price_no = {price_no_market2}")

    # Calculate arbitrage for both cases and pick the best option
    scenario_1_cost = price_yes_market1 + price_no_market2
    scenario_2_cost = price_no_market1 + price_yes_market2

    # Determine which scenario is profitable
    if scenario_1_cost < 100 or scenario_2_cost < 100:
        if scenario_1_cost < scenario_2_cost:
            profit = 100 - scenario_1_cost
            bet_type_1, bet_type_2 = "YES", "NO"
            market_bet_1, market_bet_2 = bet_id_1, bet_id_2
        else:
            profit = 100 - scenario_2_cost
            bet_type_1, bet_type_2 = "NO", "YES"
            market_bet_1, market_bet_2 = bet_id_1, bet_id_2

        print(f"Arbitrage Opportunity: Bet {bet_type_1} on {market_bet_1} ({event_1_details}), "
              f"Bet {bet_type_2} on {market_bet_2} ({event_2_details}). Profit = {profit}")
        insert_arbitrage_opportunity(connection, market_bet_1, market_bet_2, profit)
    else:
        print("No arbitrage opportunity found.")

   
# Helper - Fetch similar event IDs along with their websites
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

# Helper - Fetch event details by bet_id
def get_bet_description(bet_id: int, connection):
    """
    Fetches the name and website of the bet from the bet_description table.
    """
    query = """
    SELECT name, website
    FROM bet_description
    WHERE bet_id = %s
    """
    try:
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(query, (bet_id,))
            result = cursor.fetchone()
            if result:
                return result  # Return as a dictionary
            else:
                print(f"No bet found for bet_id {bet_id}")
                return None
    except mysql.connector.Error as e:
        print(f"Error fetching bet description for bet_id {bet_id}: {e}")
        return None


# Main script
if __name__ == "__main__":
    connection = create_connection()  # Establish the database connection

    if connection is None:
        print("Failed to connect to the database. Exiting...")
        exit()

    # Fetch all similar events from the database
    similar_event_ids = get_similar_event_ids()

    if not similar_event_ids:
        print("No similar events found for arbitrage analysis.")
        connection.close()
        exit()

    print("\nAnalyzing Arbitrage Opportunities:\n")
    
    for bet_id_1, bet_id_2 in similar_event_ids:
        # Fetch websites for both events
        event_1_details = get_bet_description(bet_id_1, connection)
        event_2_details = get_bet_description(bet_id_2, connection)

        if not event_1_details or not event_2_details:
            print(f"Missing details for bets {bet_id_1} or {bet_id_2}. Skipping...")
            continue

        website_1 = event_1_details['website']
        website_2 = event_2_details['website']

        # Calculate and display arbitrage opportunities
        calculate_cross_market_arbitrage(bet_id_1, bet_id_2, website_1, website_2, connection)

    connection.close()  # Close the database connection
    print("\nArbitrage Analysis Complete.")

