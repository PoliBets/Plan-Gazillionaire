import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

def create_connection():
    """
    Establish and return a connection to the MySQL database.
    """
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME')
        )
        print("Successfully connected to the database")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def get_option_ids_by_bet_id(connection, bet_id):
    """
    Query all options for a specific bet_id from the bet_choice table.
    Args:
        connection: MySQL connection object
        bet_id (int): The ID of the bet for which options are to be fetched.
    Returns:
        List of dictionaries containing option_id and name for the given bet_id.
    """
    query = """
    SELECT option_id, name
    FROM bet_choice
    WHERE bet_id = %s
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (bet_id,))
            results = cursor.fetchall()
            if results:
                return [{"option_id": row[0], "name": row[1]} for row in results]
            else:
                print(f"No options found for bet_id {bet_id}")
                return []
    except Error as e:
        print(f"Error fetching options for bet_id {bet_id}: {e}")
        return []

def main():
    # Establish a connection to the database
    connection = create_connection()
    if not connection:
        print("Exiting due to connection failure.")
        return

    try:
        # Example: Query options for a specific bet_id
        bet_id = int(input("Enter the bet_id to fetch options for: "))
        options = get_option_ids_by_bet_id(connection, bet_id)
        if options:
            print(f"Options for bet_id {bet_id}:")
            for option in options:
                print(f"Option ID: {option['option_id']}, Name: {option['name']}")
    finally:
        connection.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
