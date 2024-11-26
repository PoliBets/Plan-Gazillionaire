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

# Fetch similar option pairs from the database
def get_similar_option_pairs():
    """
    Fetches similar option pairs from the similar_event_options table.
    Each pair represents two options that are considered similar, and the event ID associated with them.
    """
    connection = create_connection()
    if connection is None:
        print("Failed to connect to the database.")
        return []

    query = """
    SELECT 
        option_id_1, 
        option_id_2,
        event_id
    FROM 
        similar_event_options
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            
            if not result:
                print("No similar option pairs found.")
                return []
            
            similar_option_pairs = [(row[0], row[1], row[2]) for row in result]
            print(f"Fetched similar option pairs: {similar_option_pairs}")
            return similar_option_pairs
    
    except mysql.connector.Error as e:
        print(f"Error fetching similar option pairs: {e}")
        return []
    
    finally:
        connection.close()
        print("Database connection closed.")

# Unified function to fetch prices and adjust for Polymarket
def get_prices_by_option_id(option_id: int) -> Optional[Tuple[float, float]]:
    """
    Fetch prices for a given option ID, and adjust if the bet is from Polymarket.
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
        p.option_id = %s
    ORDER BY 
        p.timestamp DESC
    LIMIT 1
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (option_id,))
            result = cursor.fetchone()
            
            if not result:
                print(f"No price data found for option_id {option_id}")
                return None, None
            
            raw_price_yes, raw_price_no, website = result
            print(f"Raw prices for option_id {option_id}: price_yes = {raw_price_yes}, price_no = {raw_price_no}, website = {website}")

            return raw_price_yes, raw_price_no
    
    except mysql.connector.Error as e:
        print(f"Error fetching prices for option_id {option_id}: {e}")
        return None, None
    
    finally:
        connection.close()
        print("Database connection closed.")

# Fetch website details using the event_id from similar_events table
def get_website_details(event_id: int, connection):
    """
    Fetches website details for a given event_id from the similar_events table.
    """
    query = """
    SELECT website_1, website_2
    FROM similar_events
    WHERE event_id = %s
    """
    try:
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(query, (event_id,))
            result = cursor.fetchone()
            if result:
                return result["website_1"], result["website_2"]
            else:
                print(f"No website data found for event_id {event_id}")
                return None, None
    except mysql.connector.Error as e:
        print(f"Error fetching website details for event_id {event_id}: {e}")
        return None, None
    
# Fetch bet_id using option_id from bet_choice table
def get_bet_id_from_option_id(option_id: int, connection) -> Optional[int]:
    query = "SELECT bet_id FROM bet_choice WHERE option_id = %s"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (option_id,))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                print(f"No bet ID found for option ID {option_id}")
                return None
    except mysql.connector.Error as e:
        print(f"Error fetching bet ID for option ID {option_id}: {e}")
        return None

# Check if bet_id exists in bet_description table
def bet_id_exists(bet_id: int, connection) -> bool:
    query = "SELECT COUNT(*) FROM bet_description WHERE bet_id = %s"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (bet_id,))
            result = cursor.fetchone()
            return result[0] > 0
    except mysql.connector.Error as e:
        print(f"Error checking if bet_id {bet_id} exists: {e}")
        return False
    
# Insert arbitrage opportunity into the database
def insert_arbitrage_opportunity(connection, option_id_1: int, option_id_2: int, profit: float):
    # Fetch the corresponding bet IDs for the given option IDs
    bet_id_1 = get_bet_id_from_option_id(option_id_1, connection)
    bet_id_2 = get_bet_id_from_option_id(option_id_2, connection)

    # Check if both bet IDs were found
    if bet_id_1 is None or bet_id_2 is None:
        print(f"Cannot insert arbitrage opportunity: One or both option IDs ({option_id_1}, {option_id_2}) could not be mapped to bet IDs.")
        return

    # Check if bet_id_1 and bet_id_2 exist in the referenced table
    if not bet_id_exists(bet_id_1, connection) or not bet_id_exists(bet_id_2, connection):
        print(f"Cannot insert arbitrage opportunity: One or both bet IDs ({bet_id_1}, {bet_id_2}) do not exist in bet_description table.")
        return

    # Insert into arbitrage_opportunities
    query = """
    INSERT INTO arbitrage_opportunities (bet_id1, bet_id2, timestamp, profit)
    VALUES (%s, %s, %s, %s)
    """
    timestamp = datetime.now()
    values = (bet_id_1, bet_id_2, timestamp, profit)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()  # Commit to save the changes
            print(f"Arbitrage opportunity added with ID: {cursor.lastrowid}")
    except Error as e:
        print(f"Error adding arbitrage opportunity: {e}")
    finally:
        print(f"Attempted to insert arbitrage opportunity: bet_id1={bet_id_1}, bet_id2={bet_id_2}, profit={profit}")


# Calculate cross-market arbitrage for a pair of option IDs
def calculate_cross_market_arbitrage(option_id_1, option_id_2, option_name_1, option_name_2, website_1, website_2, connection):
    """
    Calculate arbitrage opportunities for a pair of option IDs, considering the website.
    """
    # Ensure the options are on different platforms
    if website_1 == website_2:
        print(f"Skipping arbitrage calculation: Both options are on the same platform ({website_1}).")
        return

    # Fetch raw prices for both option IDs
    price_yes_market1, price_no_market1 = get_prices_by_option_id(option_id_1)
    price_yes_market2, price_no_market2 = get_prices_by_option_id(option_id_2)

    if None in [price_yes_market1, price_no_market1, price_yes_market2, price_no_market2]:
        print(f"Prices not available for option IDs {option_id_1} or {option_id_2}. Skipping arbitrage calculation.")
        return

    # Adjust prices based on website
    if website_1.lower() == "polymarket":
        price_yes_market1 *= 100
        price_no_market1 *= 100

    if website_2.lower() == "polymarket":
        price_yes_market2 *= 100
        price_no_market2 *= 100

    print(f"Adjusted prices for option_id_1 ({option_id_1}, {option_name_1}): price_yes = {price_yes_market1}, price_no = {price_no_market1}")
    print(f"Adjusted prices for option_id_2 ({option_id_2}, {option_name_2}): price_yes = {price_yes_market2}, price_no = {price_no_market2}")

    # Calculate arbitrage for both cases and pick the best option
    scenario_1_cost = price_yes_market1 + price_no_market2
    scenario_2_cost = price_no_market1 + price_yes_market2

    # Determine which scenario is profitable
    if scenario_1_cost < 100 or scenario_2_cost < 100:
        if scenario_1_cost < scenario_2_cost:
            profit = 100 - scenario_1_cost
            bet_type_1, bet_type_2 = "YES", "NO"
            market_bet_1, market_bet_2 = option_id_1, option_id_2
        else:
            profit = 100 - scenario_2_cost
            bet_type_1, bet_type_2 = "NO", "YES"
            market_bet_1, market_bet_2 = option_id_1, option_id_2

        print(f"Arbitrage Opportunity: Bet {bet_type_1} on {market_bet_1} ({option_name_1}), "
              f"Bet {bet_type_2} on {market_bet_2} ({option_name_2}). Profit = {profit}")
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


# Main script
if __name__ == "__main__":
    connection = create_connection()  # Establish the database connection

    if connection is None:
        print("Failed to connect to the database. Exiting...")
        exit()

    # Fetch all similar option pairs from the database
    similar_option_pairs = get_similar_option_pairs()

    if not similar_option_pairs:
        print("No similar options found for arbitrage analysis.")
        connection.close()
        exit()

    print("\nAnalyzing Arbitrage Opportunities:\n")

    for option_id_1, option_id_2, event_id in similar_option_pairs:
        # Fetch the option names from similar_event_options
        query = """
        SELECT option_name_1, option_name_2
        FROM similar_event_options
        WHERE option_id_1 = %s AND option_id_2 = %s
        """
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(query, (option_id_1, option_id_2))
            event_details = cursor.fetchone()

        if not event_details:
            print(f"No option names found for option pair ({option_id_1}, {option_id_2}). Skipping...")
            continue

        option_name_1 = event_details["option_name_1"]
        option_name_2 = event_details["option_name_2"]

        # Fetch the website information from similar_events
        website_1, website_2 = get_website_details(event_id, connection)

        if not website_1 or not website_2:
            print(f"No website information found for event_id {event_id}. Skipping...")
            continue

        # Calculate and display arbitrage opportunities for the given pair of option IDs
        calculate_cross_market_arbitrage(option_id_1, option_id_2, option_name_1, option_name_2, website_1, website_2, connection)

    connection.close()  # Close the database connection
    print("\nArbitrage Analysis Complete.")

