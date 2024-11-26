from datetime import datetime
import mysql.connector
from mysql.connector import Error
import main  

#update events to closed if they are past
def close_expired_events(connection):
    query_select = """
    SELECT bet_id, expiration_date 
    FROM bet_description 
    WHERE status = 'open'
    """
    
    query_update = """
    UPDATE bet_description 
    SET status = 'closed'
    WHERE bet_id = %s
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query_select)
            rows = cursor.fetchall()
            
            closed_events = []
            current_time = datetime.now().date()
            
            for row in rows:
                bet_id, expiration_date = row
                
                if isinstance(expiration_date, str):
                    expiration_date = datetime.strptime(expiration_date, "%Y-%m-%d %H:%M:%S")
                
                if expiration_date and expiration_date < current_time:
                    closed_events.append(bet_id)
            
            if closed_events:
                cursor.executemany(query_update, [(bet_id,) for bet_id in closed_events])
                connection.commit()
                print(f"Updated {len(closed_events)} events to 'closed' status.")
            else:
                print("No expired events found.")
    except Error as e:
        print(f"Error while updating expired events: {e}")

if __name__ == "__main__":
    connection = main.create_connection()

    if connection:
        close_expired_events(connection)
        connection.close()
        print("Database connection closed.")
    else:
        print("Failed to connect to the database.")
