import mysql.connector
from mysql.connector import Error
from difflib import SequenceMatcher
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Establish connection
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

# Calculate similarity
def calculate_similarity(string1, string2):
    return SequenceMatcher(None, string1, string2).ratio()

# Populate similar event options
def populate_similar_event_options():
    conn = create_connection()
    if conn is None:
        print("Failed to connect to the database. Exiting...")
        return

    cursor = conn.cursor()

    try:
        # Step 1: Get all event pairs from the similar_events table
        cursor.execute("SELECT event_id, bet_id_1, bet_id_2 FROM similar_events")
        similar_events = cursor.fetchall()
        print(f"Fetched {len(similar_events)} similar events.")  # Debugging

        # Step 2: For each event pair, get the options from the bet_choice table
        for event_id, bet_id_1, bet_id_2 in similar_events:
            print(f"Processing event_id: {event_id}, bet_id_1: {bet_id_1}, bet_id_2: {bet_id_2}")  # Debugging

            # Get options for bet_id_1
            cursor.execute(
                "SELECT option_id, name FROM bet_choice WHERE bet_id = %s",
                (bet_id_1,)
            )
            options_1 = cursor.fetchall()
            print(f"Fetched {len(options_1)} options for bet_id_1: {bet_id_1}")  # Debugging

            # Get options for bet_id_2
            cursor.execute(
                "SELECT option_id, name FROM bet_choice WHERE bet_id = %s",
                (bet_id_2,)
            )
            options_2 = cursor.fetchall()
            print(f"Fetched {len(options_2)} options for bet_id_2: {bet_id_2}")  # Debugging

            # Step 3: Compare options and insert similar ones into similar_event_options
            for option_1 in options_1:
                for option_2 in options_2:
                    similarity = calculate_similarity(option_1[1], option_2[1])  # Compare names
                    print(f"Comparing: '{option_1[1]}' with '{option_2[1]}', similarity: {similarity}")  # Debugging

                    if similarity >= 0.1:  # Threshold for similarity
                        print(f"Inserting into similar_event_options: event_id={event_id}, option_id_1={option_1[0]}, option_id_2={option_2[0]}")  # Debugging
                        cursor.execute(
                            """
                            INSERT INTO similar_event_options (event_id, option_id_1, option_id_2, option_name_1, option_name_2)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (event_id, option_1[0], option_2[0], option_1[1], option_2[1])
                        )

        # Commit changes to the database
        conn.commit()
        print("Similar event options have been successfully filtered and populated.")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    populate_similar_event_options()
