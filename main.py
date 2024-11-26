import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error
from datetime import datetime

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

""" *** bet_description table *** """

# create bet_description table
def create_bet_description_table(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS bet_description (
        bet_id INT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        expiration_date DATE,
        website VARCHAR(255),
        status ENUM('open', 'closed'),
        is_arbitrage ENUM('yes', 'no')
        )
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            connection.commit()
            print("Table 'bet_description' created successfully")
    except Error as e:
        print(f"Error creating table: {e}")


# Add a bet to the bet_description table
def add_bet_description(connection):
    bet_id = input("Enter bet id: ")
    name = input("Enter bet name: ")
    expiration_date = input("Enter expiration date (YYYY-MM-DD): ")  # Correct spelling here
    website = input("Enter website name: ")
    status = input("Enter bet status (open/closed): ")
    is_arbitrage = input("Enter if there were any arbitrage opportunities (yes/no): ")

    query = """
    INSERT INTO bet_description (bet_id, name, expiration_date, website, status, is_arbitrage)
    VALUES (%s, %s, %s, %s, %s, %s)
    """  
    values = (bet_id, name, expiration_date, website, status, is_arbitrage)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()
            print(f"Bet added with ID: {cursor.lastrowid}")
    except Error as e:
        print(f"Error adding bet: {e}")

# view a bet from the bet_description table
def view_bet_description(connection):
    query = "SELECT * FROM bet_description"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            if not results:
                print("No bets found in the database.")
            else:
                for bet in results:
                    print(f"\nID: {bet[0]}")
                    print(f"name: {bet[1]}")
                    print(f"expirition_date: {bet[2]}")
                    print(f"website: {bet[3]}")
                    print(f"status: ${bet[5]}")
                    print(f"is_arbitrage: ${bet[6]}")
    except Error as e:
        print(f"Error retrieving bets: {e}")

# update bets from bet_description table
def update_bet_description(connection):
    bet_id = input("Enter the ID of the bet to update: ")
    field = input("Enter the field to update (name/expirition_date/website/status/is_arbitrage): ")
    value = input("Enter the new value: ")
    
    query = f"UPDATE bet_description SET {field} = %s WHERE bet_id = %s"
    values = (value, bet_id)
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()
            if cursor.rowcount:
                print("Bet updated successfully!")
            else:
                print("No bet found with that ID.")
    except Error as e:
        print(f"Error updating bet: {e}")

# delete bet from bet_description table
def delete_bet_description(connection):
    bet_id = input("Enter the ID of the bet to delete: ")
    
    query = "DELETE FROM bet_description WHERE bet_id = %s"
    value = (bet_id,)
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, value)
            connection.commit()
            if cursor.rowcount:
                print("Bet deleted successfully!")
            else:
                print("No bet found with that ID.")
    except Error as e:
        print(f"Error deleting bet: {e}")

""" *** bet_choice table *** """

def create_bet_choice_table(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS bet_choice (
        option_id INT PRIMARY KEY,
        bet_id INT,
        name VARCHAR(255) NOT NULL,
        outcome ENUM('pending', 'win', 'lose') NOT NULL,
        FOREIGN KEY (bet_id) REFERENCES bet_description(bet_id))
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            connection.commit()
            print("Table 'bet_choice' created successfully")
    except Error as e:
        print(f"Error creating table: {e}")

def add_bet_choice(connection):
    option_id = input("Enter the option ID: ")
    bet_id = input("Enter the bet ID: ")
    name = input("Enter option name: ")
    outcome = input("Enter outcome (pending/win/lose): ")
    
    query = """
    INSERT INTO bet_choice (option_id, bet_id, name, outcome)
    VALUES (%s, %s, %s, %s)
    """
    values = (option_id, bet_id, name, outcome)
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()
            print(f"Bet choice added with ID: {cursor.lastrowid}")
    except Error as e:
        print(f"Error adding bet choice: {e}")

def view_bet_choices(connection):
    query = "SELECT * FROM bet_choice"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            if not results:
                print("No bet choices found in the database.")
            else:
                for choice in results:
                    print(f"\nOption ID: {choice[0]}")
                    print(f"Bet ID: {choice[1]}")
                    print(f"Name: {choice[2]}")
                    print(f"Outcome: {choice[3]}")
    except Error as e:
        print(f"Error retrieving bet choices: {e}")

def update_bet_choice(connection):
    option_id = input("Enter the option ID of the bet choice to update: ")
    field = input("Enter the field to update (name/outcome): ")
    new_value = input("Enter the new value: ")

    # Validate that only valid fields are updated
    if field not in ['name', 'outcome']:
        print("Invalid field. You can only update 'name' or 'outcome'.")
        return

    # Prepare the query with the dynamic field
    query = f"UPDATE bet_choice SET {field} = %s WHERE option_id = %s"
    values = (new_value, option_id)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()
            if cursor.rowcount:
                print("Bet choice updated successfully!")
            else:
                print("No bet choice found with that ID.")
    except Error as e:
        print(f"Error updating bet choice: {e}")

def delete_bet_choice(connection):
    option_id = input("Enter the option ID of the bet choice to delete: ")

    query = "DELETE FROM bet_choice WHERE option_id = %s"
    values = (option_id,)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()
            if cursor.rowcount:
                print("Bet choice deleted successfully!")
            else:
                print("No bet choice found with that ID.")
    except Error as e:
        print(f"Error deleting bet choice: {e}")


""" *** price table *** """


def create_price_table(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS price (
        option_id INT,
        timestamp DATETIME,
        volume DECIMAL(10, 2),
        yes_price DECIMAL(10, 2),
        no_price DECIMAL(10, 2),
        yes_odds DECIMAL(10, 2),
        no_odds DECIMAL(10, 2),
        PRIMARY KEY (option_id, timestamp),
        FOREIGN KEY (option_id) REFERENCES bet_choice(option_id)
    )
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            connection.commit()
            print("Table 'price' created successfully")
    except Error as e:
        print(f"Error creating table: {e}")

def add_price(connection):
    option_id = input("Enter option ID: ")
    timestamp = input("Enter timestamp (YYYY-MM-DD HH:MM:SS): ")
    volume = input("Enter volume: ")
    yes_price = input("Enter yes price: ")
    no_price = input("Enter no price: ")
    yes_odds = input("Enter yes odds: ")
    no_odds = input("Enter no odds: ")

    query = """
    INSERT INTO price (option_id, timestamp, volume, yes_price, no_price, yes_odds, no_odds)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    values = (option_id, timestamp, volume, yes_price, no_price, yes_odds, no_odds)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()
            print("Price added successfully")
    except Error as e:
        print(f"Error adding price: {e}")

def view_prices(connection):
    print("Do you want to view prices for a specific option or all options?")
    filter_choice = input("Enter 'specific' for specific option or 'all' to view all prices: ").lower()

    if filter_choice == 'specific':
        option_id = input("Enter the Option ID to view prices for: ")
        query = "SELECT * FROM price WHERE option_id = %s"
        values = (option_id,)
    elif filter_choice == 'all':
        query = "SELECT * FROM price"
        values = None
    else:
        print("Invalid choice. Please enter 'specific' or 'all'.")
        return

    try:
        with connection.cursor() as cursor:
            if values:
                cursor.execute(query, values)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            if not results:
                print("No prices found in the database.")
            else:
                for price in results:
                    print(f"\nOption ID: {price[0]}")
                    print(f"Timestamp: {price[1]}")
                    print(f"Volume: {price[2]}")
                    print(f"Yes Price: {price[3]}")
                    print(f"No Price: {price[4]}")
                    print(f"Yes Odds: {price[5]}")
                    print(f"No Odds: {price[6]}")
    except Error as e:
        print(f"Error retrieving prices: {e}")

def update_price(connection):
    option_id = input("Enter the option ID of the price to update: ")
    timestamp = input("Enter the timestamp (YYYY-MM-DD HH:MM:SS) of the price to update: ")
    field = input("Enter the field to update (volume/yes_price/no_price/yes_odds/no_odds): ")
    new_value = input(f"Enter the new value for {field}: ")

    # Validate that only valid fields are updated
    if field not in ['volume', 'yes_price', 'no_price', 'yes_odds', 'no_odds']:
        print("Invalid field. You can only update 'volume', 'yes_price', 'no_price', 'yes_odds', or 'no_odds'.")
        return

    # Prepare the query with the dynamic field
    query = f"UPDATE price SET {field} = %s WHERE option_id = %s AND timestamp = %s"
    values = (new_value, option_id, timestamp)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()
            if cursor.rowcount:
                print("Price updated successfully!")
            else:
                print("No price found with that Option ID and Timestamp.")
    except Error as e:
        print(f"Error updating price: {e}")

def delete_price(connection):
    option_id = input("Enter the option ID of the price to delete: ")
    timestamp = input("Enter the timestamp (YYYY-MM-DD HH:MM:SS) of the price to delete: ")

    query = "DELETE FROM price WHERE option_id = %s AND timestamp = %s"
    values = (option_id, timestamp)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()
            if cursor.rowcount:
                print("Price deleted successfully!")
            else:
                print("No price found with that Option ID and Timestamp.")
    except Error as e:
        print(f"Error deleting price: {e}")

""" *** arbitrage_opportunities table *** """

def create_arbitrage_opportunities_table(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
        arb_id INT AUTO_INCREMENT PRIMARY KEY,
        bet_id_1 INT NOT NULL,
        bet_id_2 INT NOT NULL,
        timestamp DATETIME,
        profit DECIMAL(10, 2),
        FOREIGN KEY (bet_id_1) REFERENCES bet_description(bet_id),
        FOREIGN KEY (bet_id_2) REFERENCES bet_description(bet_id)
    )
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            connection.commit()
            print("Table 'arbitrage_opportunities' created successfully")
    except Error as e:
        print(f"Error creating table: {e}")

def add_arbitrage_opportunity(connection):
    bet_id_1 = input("Enter the first bet ID (bet_id_1): ")
    bet_id_2 = input("Enter the second bet ID (bet_id_2): ")
    timestamp = datetime.now().strftime('%Y-%m-%d')
    profit = input("Enter the profit (decimal value): ")

    query = """
    INSERT INTO arbitrage_opportunities (bet_id_1, bet_id_2, timestamp, profit)
    VALUES (%s, %s, %s, %s)
    """
    values = (bet_id_1, bet_id_2, timestamp, profit)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()
            print(f"Arbitrage opportunity added with ID: {cursor.lastrowid}")
    except Error as e:
        print(f"Error adding arbitrage opportunity: {e}")

def view_arbitrage_opportunities(connection):
    query = "SELECT * FROM arbitrage_opportunities"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            if not results:
                print("No arbitrage opportunities found in the database.")
            else:
                for arb in results:
                    print(f"\nID: {arb[0]}")
                    print(f"Bet ID 1: {arb[1]}")
                    print(f"Bet ID 2: {arb[2]}")
                    print(f"Timestamp: {arb[3]}")
                    print(f"Profit: {arb[4]}")
    except Error as e:
        print(f"Error retrieving arbitrage opportunities: {e}")

def update_arbitrage_opportunity(connection):
    arb_id = input("Enter the ID of the arbitrage opportunity to update: ")
    field = input("Enter the field to update (bet_id_1/bet_id_2/timestamp/profit): ")
    value = input("Enter the new value: ")

    query = f"UPDATE arbitrage_opportunities SET {field} = %s WHERE arb_id = %s"
    values = (value, arb_id)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()
            if cursor.rowcount:
                print("Arbitrage opportunity updated successfully!")
            else:
                print("No arbitrage opportunity found with that ID.")
    except Error as e:
        print(f"Error updating arbitrage opportunity: {e}")

def delete_arbitrage_opportunity(connection):
    arb_id = input("Enter the ID of the arbitrage opportunity to delete: ")

    query = "DELETE FROM arbitrage_opportunities WHERE arb_id = %s"
    value = (arb_id,)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, value)
            connection.commit()
            if cursor.rowcount:
                print("Arbitrage opportunity deleted successfully!")
            else:
                print("No arbitrage opportunity found with that ID.")
    except Error as e:
        print(f"Error deleting arbitrage opportunity: {e}")

def main_menu(connection):
    while True:
        print("\nMain Menu:")
        print("1. Manage Bet Descriptions")
        print("2. Manage Bet Choices")
        print("3. Manage Prices")
        print("4. Manage Arbitrage Opportunities")
        print("5. Manage Similar Events")  # New option
        print("6. Manage Similar Options")
        print("7. Exit")
        
        choice = input("Enter your choice (1-7): ")

        if choice == '1':
            manage_bet_description(connection)
        elif choice == '2':
            manage_bet_choice(connection)
        elif choice == '3':
            manage_prices(connection)
        elif choice == '4':
            manage_arbitrage_opportunities(connection)
        elif choice == '5':
            manage_similar_events(connection)  # New option
        elif choice == '6':
            manage_similar_options(connection)
        elif choice == '7':
            break
        else:
            print("Invalid choice. Please try again.")
    
    connection.close()
    print("Connection Closed")

""" *** similar_events table *** """

def create_similar_events_table(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS similar_events (
        event_id INT AUTO_INCREMENT PRIMARY KEY,
        bet_id_1 INT NOT NULL,
        description_1 TEXT NOT NULL,
        website_1 VARCHAR(255) NOT NULL,
        bet_id_2 INT NOT NULL,
        description_2 TEXT NOT NULL,
        website_2 VARCHAR(255) NOT NULL,
        FOREIGN KEY (bet_id_1) REFERENCES bet_description(bet_id),
        FOREIGN KEY (bet_id_2) REFERENCES bet_description(bet_id)
    )
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            connection.commit()
            print("Table 'similar_events' created successfully with updated schema!")
    except Error as e:
        print(f"Error creating table 'similar_events': {e}")

def add_similar_event(connection):
    print("\nEnter details for the first event in the pair:")
    bet_id_1 = input("Enter Bet ID 1: ").strip()
    description_1 = input("Enter Description 1: ").strip()
    website_1 = input("Enter Website for Bet 1 (e.g., Polymarket): ").strip()
    
    print("\nEnter details for the second event in the pair:")
    bet_id_2 = input("Enter Bet ID 2: ").strip()
    description_2 = input("Enter Description 2: ").strip()
    website_2 = input("Enter Website for Bet 2 (e.g., Kalshi): ").strip()

    # Validate input
    if not all([bet_id_1, description_1, website_1, bet_id_2, description_2, website_2]):
        print("All fields are required. Please try again.")
        return
    if not (bet_id_1.isdigit() and bet_id_2.isdigit()):
        print("Bet IDs must be numeric. Please try again.")
        return

    query = """
    INSERT INTO similar_events (bet_id_1, description_1, website_1, bet_id_2, description_2, website_2)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    values = (bet_id_1, description_1, website_1, bet_id_2, description_2, website_2)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()
            print("Similar event pair added successfully!")
    except Error as e:
        print(f"Error adding similar event pair: {e}")

def view_similar_events(connection):
    query = "SELECT * FROM similar_events"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            if not results:
                print("No similar event pairs found in the database.")
            else:
                for event in results:
                    print(f"\nEvent ID: {event[0]}")
                    print(f"Bet ID 1: {event[1]}")
                    print(f"Description 1: {event[2]}")
                    print(f"Website 1: {event[3]}")
                    print(f"Bet ID 2: {event[4]}")
                    print(f"Description 2: {event[5]}")
                    print(f"Website 2: {event[6]}")
    except Error as e:
        print(f"Error retrieving similar events: {e}")

def update_bet_id_in_similar_event(connection):
    """
    Update the bet_id_1 or bet_id_2 in a specific similar event pair.
    """
    event_id = input("Enter the Event ID of the similar event pair to update: ").strip()

    # Validate input
    if not event_id.isdigit():
        print("Event ID must be numeric. Please try again.")
        return

    print("Which Bet ID would you like to update?")
    print("1. Update Bet ID 1")
    print("2. Update Bet ID 2")
    print("3. Exit")

    try:
        while True:
            choice = input("Enter your choice (1-3): ").strip()

            if choice == "1":
                new_bet_id_1 = input("Enter the new Bet ID 1: ").strip()
                if not new_bet_id_1.isdigit():
                    print("Bet ID must be numeric. Please try again.")
                    continue
                query = "UPDATE similar_events SET bet_id_1 = %s WHERE event_id = %s"
                values = (new_bet_id_1, event_id)

            elif choice == "2":
                new_bet_id_2 = input("Enter the new Bet ID 2: ").strip()
                if not new_bet_id_2.isdigit():
                    print("Bet ID must be numeric. Please try again.")
                    continue
                query = "UPDATE similar_events SET bet_id_2 = %s WHERE event_id = %s"
                values = (new_bet_id_2, event_id)

            elif choice == "3":
                print("Exiting update menu.")
                break

            else:
                print("Invalid choice. Please enter 1, 2, or 3.")
                continue

            # Execute the update query
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, values)
                    connection.commit()
                    if cursor.rowcount > 0:
                        print("Bet ID updated successfully!")
                    else:
                        print("No similar event pair found with the given Event ID.")
                break  # Exit after a successful update

            except Error as e:
                print(f"Error updating similar event pair: {e}")
                continue

    except Exception as e:
        print(f"Error: {e}")

def delete_similar_event(connection):
    event_id = input("\nEnter the Event ID of the similar event pair to delete: ").strip()

    # Validate input
    if not event_id.isdigit():
        print("Event ID must be numeric. Please try again.")
        return

    query = "DELETE FROM similar_events WHERE event_id = %s"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (event_id,))
            connection.commit()
            if cursor.rowcount > 0:
                print(f"Similar event pair with Event ID {event_id} deleted successfully!")
            else:
                print(f"No similar event pair found with Event ID {event_id}.")
    except Error as e:
        print(f"Error deleting similar event pair: {e}")

def manage_similar_events(connection):
    while True:
        print("\nSimilar Events Management:")
        print("1. Add a Similar Event Pair")
        print("2. View Similar Event Pairs")
        print("3. Update a Similar Event Pair")
        print("4. Delete a Similar Event Pair")
        print("5. Go Back to Main Menu")
        
        choice = input("Enter your choice (1-4): ").strip()

        if choice == '1':
            add_similar_event(connection)
        elif choice == '2':
            view_similar_events(connection)
        elif choice == '3':
            update_bet_id_in_similar_event(connection)
        elif choice == '4':
            delete_similar_event(connection)
        elif choice == '5':
            break
        else:
            print("Invalid choice. Please enter 1, 2, 3, 4, or 5.")

""" *** similar_event_options table *** """
def create_similar_event_options_table(connection):
    """
    Drops and creates the `similar_event_options` table in the database.
    This table links similar_events with corresponding options from bet_choice.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS similar_event_options (
        id INT AUTO_INCREMENT PRIMARY KEY,
        event_id INT NOT NULL,
        option_id_1 INT NOT NULL,
        option_id_2 INT NOT NULL,
        option_name_1 VARCHAR(255),
        option_name_2 VARCHAR(255),
        FOREIGN KEY (event_id) REFERENCES similar_events(event_id),
        FOREIGN KEY (option_id_1) REFERENCES bet_choice(option_id),
        FOREIGN KEY (option_id_2) REFERENCES bet_choice(option_id)
    );
    """
    try:
        with connection.cursor() as cursor:
            # Create new table
            cursor.execute(create_table_query)
            connection.commit()
            print("Table 'similar_event_options' recreated successfully.")
    except Error as e:
        print(f"Error creating 'similar_event_options' table: {e}")

def add_similar_event_options(connection):
    """
    Add paired options for a given similar event (event_id) manually with option names.
    """
    event_id = input("Enter the event_id for the similar event: ").strip()

    # Validate event_id
    if not event_id.isdigit():
        print("Invalid event_id. Please try again.")
        return

    # Add first and second options
    option_id_1 = input("Enter option_id for the first event: ").strip()
    option_id_2 = input("Enter option_id for the second event: ").strip()

    # Validate option_ids
    if not option_id_1.isdigit() or not option_id_2.isdigit():
        print("Invalid option IDs. Please try again.")
        return

    # Manually input the option names
    option_name_1 = input("Enter the name for the first option: ").strip()
    option_name_2 = input("Enter the name for the second option: ").strip()

    # Insert data into similar_event_options table
    query = """
    INSERT INTO similar_event_options (event_id, option_id_1, option_id_2, option_name_1, option_name_2)
    VALUES (%s, %s, %s, %s, %s);
    """
    values = (event_id, option_id_1, option_id_2, option_name_1, option_name_2)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            connection.commit()
            print("Similar event options added successfully!")
    except Error as e:
        print(f"Error adding similar event options: {e}")

# View all similar option pairs
def view_similar_option_pairs(connection):
    query = """
    SELECT seo.id, seo.event_id, se.description_1, seo.option_id_1, seo.option_name_1, seo.option_id_2, seo.option_name_2
    FROM similar_event_options seo
    JOIN similar_events se ON seo.event_id = se.event_id
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            if not results:
                print("No similar option pairs found in the database.")
            else:
                for record in results:
                    print(f"\nID: {record[0]}")
                    print(f"Event ID: {record[1]}")
                    print(f"Event Description: {record[2]}")
                    print(f"Option ID 1: {record[3]}")
                    print(f"Option Name 1: {record[4]}")
                    print(f"Option ID 2: {record[5]}")
                    print(f"Option Name 2: {record[6]}")
    except Error as e:
        print(f"Error retrieving similar option pairs: {e}")

# Delete a similar option pair by its ID
def delete_similar_option_pair(connection):
    try:
        pair_id = input("Enter the ID of the similar option pair to delete: ").strip()
        if not pair_id.isdigit():
            print("Invalid input. The ID must be numeric.")
            return

        query = "DELETE FROM similar_event_options WHERE id = %s;"
        with connection.cursor() as cursor:
            cursor.execute(query, (pair_id,))
            connection.commit()
            if cursor.rowcount > 0:
                print("Similar option pair deleted successfully.")
            else:
                print("No similar option pair found with the given ID.")
    except Error as e:
        print(f"Error deleting similar option pair: {e}")

# Manage similar options menu
def manage_similar_options(connection):
    while True:
        print("\nManage Similar Options:")
        print("1. Add a Similar Option Pair")
        print("2. View Similar Option Pairs")
        print("3. Delete a Similar Option Pair")
        print("4. Back to Main Menu")

        choice = input("Enter your choice (1-4): ").strip()

        if choice == '1':
            add_similar_event_options(connection)
        elif choice == '2':
            view_similar_option_pairs(connection)
        elif choice == '3':
            delete_similar_option_pair(connection)
        elif choice == '4':
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 4.")

""" *** sub-menu for Best Choice *** """

def manage_bet_choice(connection):
    while True:
        print("\nBet Choices Management:")
        print("1. Add a Bet Choice")
        print("2. View all Bet Choices")
        print("3. Update a Bet Choice")
        print("4. Delete a Bet Choice")
        print("5. Go Back to Main Menu")
        
        choice = input("Enter your choice (1-5): ")

        if choice == '1':
            add_bet_choice(connection)
        elif choice == '2':
            view_bet_choices(connection)
        elif choice == '3':
            update_bet_choice(connection)
        elif choice == '4':
            delete_bet_choice(connection)
        elif choice == '5':
            break
        else:
            print("Invalid choice. Please try again.")

""" *** sub-menu for Prices *** """

def manage_prices(connection):
    while True:
        print("\nPrices Management:")
        print("1. Add a Price")
        print("2. View Prices")
        print("3. Update a Price")
        print("4. Delete a Price")
        print("5. Go Back to Main Menu")
        
        choice = input("Enter your choice (1-5): ")

        if choice == '1':
            add_price(connection)
        elif choice == '2':
            view_prices(connection)
        elif choice == '3':
            update_price(connection)
        elif choice == '4':
            delete_price(connection)
        elif choice == '5':
            break
        else:
            print("Invalid choice. Please try again.")

""" *** sub-menu for Arbitrage Opportunities *** """

def manage_arbitrage_opportunities(connection):
    while True:
        print("\nArbitrage Opportunities Management:")
        print("1. Add an Arbitrage Opportunity")
        print("2. View Arbitrage Opportunities")
        print("3. Update an Arbitrage Opportunity")
        print("4. Delete an Arbitrage Opportunity")
        print("5. Go Back to Main Menu")
        
        choice = input("Enter your choice (1-5): ")

        if choice == '1':
            add_arbitrage_opportunity(connection)
        elif choice == '2':
            view_arbitrage_opportunities(connection)
        elif choice == '3':
            update_arbitrage_opportunity(connection)
        elif choice == '4':
            delete_arbitrage_opportunity(connection)
        elif choice == '5':
            break
        else:
            print("Invalid choice. Please try again.")
            
def join_bet_data(connection):
    query = """
    SELECT 
        bd.bet_id, 
        bd.name AS bet_name, 
        bd.expiration_date, 
        bd.website, 
        bd.event_type, 
        bd.status, 
        bd.is_arbitrage,
        bc.option_id, 
        bc.name AS option_name, 
        bc.outcome, 
        p.timestamp AS price_timestamp, 
        p.volume, 
        p.yes_price, 
        p.no_price, 
        p.yes_odds, 
        p.no_odds, 
        ao.arb_id, 
        ao.bet_id_1, 
        ao.bet_id_2, 
        ao.timestamp AS arbitrage_timestamp, 
        ao.profit
    FROM 
        bet_description bd
    JOIN 
        bet_choice bc ON bd.bet_id = bc.bet_id
    JOIN 
        price p ON bc.option_id = p.option_id
    LEFT JOIN 
        arbitrage_opportunities ao ON bd.bet_id = ao.bet_id_1 OR bd.bet_id = ao.bet_id_2;
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

            if not results:
                print("No data found in the database.")
            else:
                for row in results:
                    print(f"Bet ID: {row[0]}")
                    print(f"Bet Name: {row[1]}")
                    print(f"Expiration Date: {row[2]}")
                    print(f"Website: {row[3]}")
                    print(f"Event Type: {row[4]}")
                    print(f"Status: {row[5]}")
                    print(f"Is Arbitrage: {row[6]}")
                    print(f"Option ID: {row[7]}")
                    print(f"Option Name: {row[8]}")
                    print(f"Outcome: {row[9]}")
                    print(f"Price Timestamp: {row[10]}")
                    print(f"Volume: {row[11]}")
                    print(f"Yes Price: {row[12]}")
                    print(f"No Price: {row[13]}")
                    print(f"Yes Odds: {row[14]}")
                    print(f"No Odds: {row[15]}")
                    print(f"Arbitrage ID: {row[16]}")
                    print(f"Arbitrage Bet ID 1: {row[17]}")
                    print(f"Arbitrage Bet ID 2: {row[18]}")
                    print(f"Arbitrage Timestamp: {row[19]}")
                    print(f"Arbitrage Profit: {row[20]}")
                    print("-------------------------")
    except Error as e:
        print(f"Error retrieving data: {e}")

def bet_exists(connection, bet_id):
    """
    Checks if an bet with the specified bet_id already exists in the database.
    Returns:
    - True if the bet exists, False otherwise
    """
    try:
        cursor = connection.cursor()
        query = "SELECT 1 FROM bet_description WHERE bet_id = %s LIMIT 1"
        cursor.execute(query, (bet_id,))
        return cursor.fetchone() is not None  # Returns True if record exists
    except Error as e:
        print(f"Error checking for existing event: {e}")
        return False
    finally:
        cursor.close()

def option_exists(connection, option_id):
    """
    Checks if an option with the specified option_id already exists in the database.
    Returns:
    - True if the option exists, False otherwise
    """
    try:
        cursor = connection.cursor()
        query = "SELECT 1 FROM bet_choice WHERE option_id = %s LIMIT 1"
        cursor.execute(query, (option_id,))
        return cursor.fetchone() is not None  # Returns True if record exists
    except Error as e:
        print(f"Error checking for existing event: {e}")
        return False
    finally:
        cursor.close()

def price_exists(connection, option_id):
    """
    Checks if an price with the specified option_id already exists in the database.
    Returns:
    - True if the price exists, False otherwise
    """
    try:
        cursor = connection.cursor()
        query = "SELECT 1 FROM price WHERE option_id = %s LIMIT 1"
        cursor.execute(query, (option_id,))
        return cursor.fetchone() is not None  # Returns True if record exists
    except Error as e:
        print(f"Error checking for existing event: {e}")
        return False
    finally:
        cursor.close()

""" *** main *** """

def main():
    connection = create_connection()

    if connection is None:
        print("Error: Could not establish a database connection.")
    else:
        try:
            create_bet_description_table(connection)
            create_bet_choice_table(connection)
            create_price_table(connection)
            create_arbitrage_opportunities_table(connection)
            create_similar_events_table(connection)
            create_similar_event_options_table(connection)
            join_bet_data(connection)

            main_menu(connection)

        finally:
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
