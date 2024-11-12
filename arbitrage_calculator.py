import requests
from requests.adapters import HTTPAdapter
from typing import Optional, Tuple
from requests import Session
from requests.packages.urllib3.util.retry import Retry
from app import SessionLocal
from similar_events import similar_event_ids  # Import the similar event pairs
from app import BetDescription, Price # Import BetDescription and Price tables from API
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

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
        print(f"Attempting to connect to:")
        print(f"Host: {os.getenv('DB_HOST')}")
        print(f"User: {os.getenv('DB_USER')}")
        print(f"Database: {os.getenv('DB_NAME')}")
        
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

ARBITRAGE_BASE_URL = "http://127.0.0.1:9000/api/v1/arbitrage"
def post_arbitrage_opportunity(bet_id1: int, bet_id2: int, profit: float):
    data = {
        "bet_id1": bet_id1,
        "bet_id2": bet_id2,
        "timestamp": None,  # or set to the current datetime if required
        "profit": float(profit)
    }
    try:
        response = requests.post(ARBITRAGE_BASE_URL, json=data)
        response.raise_for_status()  # Raises an error for non-2xx responses
        print(f"Successfully added arbitrage opportunity for bet IDs {bet_id1} and {bet_id2}")
    except requests.RequestException as e:
        print(f"Failed to add arbitrage opportunity for bet IDs {bet_id1} and {bet_id2}: {e}")

# Calculate cross-market arbitrage for a pair of bet IDs
def calculate_cross_market_arbitrage(bet_id1, bet_id2):
    price_yes_market1, price_no_market1 = get_prices_by_bet_id(bet_id1)
    price_yes_market2, price_no_market2 = get_prices_by_bet_id(bet_id2)
    
    if None in [price_yes_market1, price_no_market1, price_yes_market2, price_no_market2]:
        print(f"Prices not available for bet IDs {bet_id1} or {bet_id2}. Skipping arbitrage calculation.")
        return None

    # Scenario 1: "Yes" on Market 1, "No" on Market 2
    total_price_scenario_1 = price_yes_market1 + price_no_market2
    profit_scenario_1 = 100 - total_price_scenario_1 if total_price_scenario_1 < 100 else 0

    # Scenario 2: "No" on Market 1, "Yes" on Market 2
    total_price_scenario_2 = price_no_market1 + price_yes_market2
    profit_scenario_2 = 100 - total_price_scenario_2 if total_price_scenario_2 < 100 else 0

    if profit_scenario_1 > 0 or profit_scenario_2 > 0:

        # Determine which scenario has a profitable arbitrage opportunity and add to the API
        if profit_scenario_1 > 0:
            post_arbitrage_opportunity(bet_id1, bet_id2, profit_scenario_1)
        if profit_scenario_2 > 0:
            post_arbitrage_opportunity(bet_id2, bet_id1, profit_scenario_2)
        
        return {
            "arbitrage": True,
            "bet_ids": (bet_id1, bet_id2),
            "scenario_1": {
                "bet_on": ("Yes on Market 1", "No on Market 2"),
                "total_investment": total_price_scenario_1,
                "profit_per_contract": profit_scenario_1
            },
            "scenario_2": {
                "bet_on": ("No on Market 1", "Yes on Market 2"),
                "total_investment": total_price_scenario_2,
                "profit_per_contract": profit_scenario_2
            }
        }
    else:
        return {
            "arbitrage": False,
            "bet_ids": (bet_id1, bet_id2),
            "message": "No arbitrage opportunity available in either scenario."
        }

# Main script
if __name__ == "__main__":
    arbitrage_results = []
    for bet_id1, bet_id2, similarity_score in similar_event_ids:
        result = calculate_cross_market_arbitrage(bet_id1, bet_id2)
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



"""
def get_bet_prices(bet_id: int) -> Optional[Tuple[float, float]]:
    # Start a new session using SessionLocal
    db: Session = SessionLocal()
    try:
        # Fetch the most recent price entry for the given bet_id
        recent_price = db.query(Price).filter(Price.option_id == bet_id).order_by(Price.timestamp.desc()).first()
        
        if not recent_price:
            print(f"No price data found for bet_id {bet_id}")
            return None, None

        # Extract price values (adjust field names based on your actual model)
        price_yes = recent_price.yes_price
        price_no = recent_price.no_price
        
        print(f"Fetched prices for bet_id {bet_id}: price_yes = {price_yes}, price_no = {price_no}")
        
        return price_yes, price_no
    
    except Exception as e:
        print(f"Error fetching prices for bet_id {bet_id}: {e}")
        return None, None
    finally:
        # Ensure the session is closed after operation
        db.close()
"""

"""
# Debug code ==> DELETE when done
def get_bet_prices(bet_id: int) -> Optional[Tuple[float, float]]:
    # Start a new session using SessionLocal
    db: Session = SessionLocal()
    try:
        # Fetch the most recent price entry for the given bet_id
        recent_price = db.query(Price).filter(Price.option_id == bet_id).order_by(Price.timestamp.desc()).first()
        
        if not recent_price:
            print(f"No price data found for bet_id {bet_id}")
            
            # Optional: Attempt API call for debugging
            print("Attempting to fetch data from API for debugging...")
            response = requests.get(f"{API_BASE_URL}/{bet_id}", timeout=5)
            print(f"Status Code: {response.status_code}")
            print(f"Response Content: {response.text}")
            
            try:
                response.raise_for_status()
                bet_data = response.json()
                print(f"Fetched JSON data for bet_id {bet_id}: {bet_data}")
                return None, None  # Replace with actual processing if you want to handle the API response
            except requests.RequestException as e:
                print(f"Error with API request for bet_id {bet_id}: {e}")
                return None, None
            
        # Print the full database record for inspection
        print(f"Full data retrieved from database for bet_id {bet_id}: {recent_price}")
        
        # Extract price values
        price_yes = recent_price.yes_price
        price_no = recent_price.no_price
        print(f"Fetched prices for bet_id {bet_id}: price_yes = {price_yes}, price_no = {price_no}")
        
        return price_yes, price_no
"""

"""
def get_prices_by_bet_id(db: Session, bet_id: int) -> Optional[Tuple[float, float]]:
    try:
        # Perform a join to retrieve the most recent price for a specific bet_id
        recent_price = (
            db.query(Price)
            .join(BetChoice, Price.option_id == BetChoice.option_id)
            .filter(BetChoice.bet_id == bet_id)
            .order_by(Price.timestamp.desc())
            .first()
        )
        
        if not recent_price:
            print(f"No price data found for bet_id {bet_id}")
            return None, None
        
        # Print the full database record for inspection
        print(f"Full data retrieved from database for bet_id {bet_id}: {recent_price}")
        
        # Extract price values
        price_yes = recent_price.yes_price
        price_no = recent_price.no_price
        print(f"Fetched prices for bet_id {bet_id}: price_yes = {price_yes}, price_no = {price_no}")
        
        return price_yes, price_no
    
    except Exception as e:
        print(f"Error fetching prices for bet_id {bet_id}: {e}")
        return None, None

    
    except Exception as e:
        print(f"Error fetching prices for bet_id {bet_id}: {e}")
        return None, None
    finally:
        # Ensure the session is closed after operation
        db.close()
"""
