import requests
import math
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
from globals import add_to_arbitrage_sides_lookup

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
        #print("Successfully connected to the database")
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
        #print("Database connection closed.")

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
        #print("Database connection closed.")

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
def insert_arbitrage_opportunity(connection, bet_id_1: int, bet_id_2: int, profit: float, bet_side_1: str, bet_side_2: str):
    # Fetch the corresponding bet IDs for the given option IDs
    bet_id_1 = get_bet_id_from_option_id(bet_id_1, connection)
    bet_id_2 = get_bet_id_from_option_id(bet_id_2, connection)

    # Check if both bet IDs were found
    if bet_id_1 is None or bet_id_2 is None:
        print(f"Cannot insert arbitrage opportunity: One or both option IDs ({bet_id_1}, {bet_id_2}) could not be mapped to bet IDs.")
        return

    # Check if bet_id_1 and bet_id_2 exist in the referenced table
    if not bet_id_exists(bet_id_1, connection) or not bet_id_exists(bet_id_2, connection):
        print(f"Cannot insert arbitrage opportunity: One or both bet IDs ({bet_id_1}, {bet_id_2}) do not exist in bet_description table.")
        return

    # Insert into arbitrage_opportunities
    arbitrage_query = """
    INSERT INTO arbitrage_opportunities (bet_id1, bet_id2, timestamp, profit)
    VALUES (%s, %s, %s, %s)
    """
    timestamp = datetime.now()
    arbitrage_values = (bet_id_1, bet_id_2, timestamp, profit)

    try:
        with connection.cursor() as cursor:
            # Insert into arbitrage_opportunities table
            cursor.execute(arbitrage_query, arbitrage_values)
            arb_id = cursor.lastrowid
            connection.commit()

            # Insert into arbitrage_bet_sides table
            bet_sides_query = """
            INSERT INTO arbitrage_bet_sides (arb_id, bet_side_1, bet_side_2)
            VALUES (%s, %s, %s)
            """
            bet_sides_values = (arb_id, bet_side_1, bet_side_2)

            cursor.execute(bet_sides_query, bet_sides_values)
            connection.commit()

            # Optionally update the in-memory lookup as well
            add_to_arbitrage_sides_lookup(arb_id, bet_side_1, bet_side_2)

            print(f"Inserted arbitrage sides for arb_id {arb_id}")
    except Error as e:
        print(f"Error adding arbitrage opportunity: {e}")
    finally:
        print(f"Attempted to insert arbitrage opportunity: bet_id1={bet_id_1}, bet_id2={bet_id_2}, profit={profit}")

# Calculate Kalshi fees
def calculate_kalshi_total_cost(price):
    """
    Calculate the total cost for Kalshi, including fees, keeping costs in the 0–100 scale.

    Args:
        price (float): The raw price of the contract as a percentage (0–100).

    Returns:
        float: Total cost (option price + fees) in the 0–100 scale.
    """
    # Convert price to probability (0–1 scale) for fee calculation
    p = float(price) / 100.0  # Example: 3.00 -> 0.03

    # Calculate Kalshi fees using F = θ * p * (1 - p)
    theta = 0.07
    fee = theta * p * (1 - p) * 100  # Scale back to 0–100

    # Total cost = Option price + Fee (both in 0–100 scale)
    total_cost = price + fee

    return total_cost

# Calculate cross-market arbitrage for a pair of option IDs
def calculate_cross_market_arbitrage(option_id_1, option_id_2, option_name_1, option_name_2, website_1, website_2, connection, initial_amount=100):
    """
    Calculate arbitrage opportunities for a pair of option IDs, considering the website.
    Assumes a fixed initial amount of $100 for each trade.
    """
    # Ensure the options are on different platforms
    if website_1 == website_2:
        print(f"Skipping arbitrage calculation: Both options are on the same platform ({website_1}).")
        return

    # Fetch raw prices for both option IDs
    price_yes_market1, price_no_market1 = map(float, get_prices_by_option_id(option_id_1))
    price_yes_market2, price_no_market2 = map(float, get_prices_by_option_id(option_id_2))

    if None in [price_yes_market1, price_no_market1, price_yes_market2, price_no_market2]:
        print(f"Prices not available for option IDs {option_id_1} or {option_id_2}. Skipping arbitrage calculation.")
        return

    if website_1.lower() == "kalshi":
        price_yes_market1 = float(price_yes_market1)
        price_no_market1 = float(price_no_market1)

    if website_2.lower() == "kalshi":
        price_yes_market2 = float(price_yes_market2)
        price_no_market2 = float(price_no_market2)

    # Fixed number of contracts assuming $100 for each trade
    def calculate_contracts(price, initial_amount=100):
        """
        Fix the number of contracts for each trade to 1.

        Args:
            price (float): The price of the contract (0-100).
            initial_amount (float): Ignored in this case since contracts are fixed.

        Returns:
            int: Fixed number of contracts (1).
        """
        return 1  # Always one contract per trade.

    # Calculate contracts for each side
    C1_yes = calculate_contracts(price_yes_market1)
    C1_no = calculate_contracts(price_no_market1)
    C2_yes = calculate_contracts(price_yes_market2)
    C2_no = calculate_contracts(price_no_market2)

# Calculate Kalshi's total cost (option price + fees) directly
    total_cost_yes_market1 = float(calculate_kalshi_total_cost(price_yes_market1)) if website_1.lower() == "kalshi" else float(price_yes_market1)
    total_cost_no_market1 = float(calculate_kalshi_total_cost(price_no_market1)) if website_1.lower() == "kalshi" else float(price_no_market1)

    total_cost_yes_market2 = float(calculate_kalshi_total_cost(price_yes_market2)) if website_2.lower() == "kalshi" else float(price_yes_market2)
    total_cost_no_market2 = float(calculate_kalshi_total_cost(price_no_market2)) if website_2.lower() == "kalshi" else float(price_no_market2)

    scenario_1_cost_with_fees = float(total_cost_yes_market1) + float(total_cost_no_market2)
    scenario_2_cost_with_fees = float(total_cost_no_market1) + float(total_cost_yes_market2)

    # Print detailed costs and fees for both platforms
    print_market_details(
        market_name="Market 1",
        website=website_1,
        yes_cost=total_cost_yes_market1,
        no_cost=total_cost_no_market1,
        yes_fees=0,  # Fees are already included in total cost
        no_fees=0,
    )

    print_market_details(
        market_name="Market 2",
        website=website_2,
        yes_cost=total_cost_yes_market2,
        no_cost=total_cost_no_market2,
        yes_fees=0,  # Fees are already included in total cost
        no_fees=0,
    )

    
    # Determine which scenario is profitable
    if scenario_1_cost_with_fees < 100 or scenario_2_cost_with_fees < 100:
        if scenario_1_cost_with_fees < scenario_2_cost_with_fees:
            profit = 100 - scenario_1_cost_with_fees
            bet_type_1, bet_type_2 = "YES", "NO"
            market_bet_1, market_bet_2 = option_id_1, option_id_2
        else:
            profit = 100 - scenario_2_cost_with_fees
            bet_type_1, bet_type_2 = "NO", "YES"
            market_bet_1, market_bet_2 = option_id_1, option_id_2

        print(f"Arbitrage Opportunity: Bet {bet_type_1} on {market_bet_1} ({option_name_1}), "
              f"Bet {bet_type_2} on {market_bet_2} ({option_name_2}). Profit = ${profit:.2f}")
        
        # Pass the bet sides to insert_arbitrage_opportunity()
        insert_arbitrage_opportunity(connection, market_bet_1, market_bet_2, profit, bet_type_1, bet_type_2)
    else:
        print("\nNo arbitrage opportunity found.\n")

# Print detailed costs and fees for both platforms
def print_market_details(market_name, website, yes_cost, no_cost, yes_fees, no_fees):
    """
    Print market details including costs and fees. Fees are printed only for Kalshi.
    
    Args:
        market_name (str): Market identifier (e.g., "Market 1" or "Market 2").
        website (str): Platform name (e.g., "kalshi" or "polymarket").
        yes_cost (float): Cost of YES contracts.
        no_cost (float): Cost of NO contracts.
        yes_fees (float): Fees for YES contracts (only for Kalshi).
        no_fees (float): Fees for NO contracts (only for Kalshi).
    """
    print()
    print(f"{market_name} ({website.capitalize()}):")
    print(f"  YES contracts cost: ${yes_cost:.2f}", end="")
    if website.lower() == "kalshi":
        print(f", Fees: ${yes_fees:.2f}")
    else:
        print()
    print(f"  NO contracts cost: ${no_cost:.2f}", end="")
    if website.lower() == "kalshi":
        print(f", Fees: ${no_fees:.2f}")
    else:
        print()
   
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
        #print("Database connection closed.")


# Main script
def update_arbitrage():
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
        print()
        calculate_cross_market_arbitrage(option_id_1, option_id_2, option_name_1, option_name_2, website_1, website_2, connection)

    connection.close()  # Close the database connection
    print("\nArbitrage Analysis Complete.")

