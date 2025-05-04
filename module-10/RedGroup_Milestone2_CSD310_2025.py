# Red Group
# Dario Gomez, Juedeja Richard, Kristopher Kuenning, Scott Macioce

import mysql.connector
from mysql.connector import errorcode
from dotenv import dotenv_values

# Load secrets from .env
secrets = dotenv_values(".env")

# Database configuration
config = {
    "user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
    "raise_on_warnings": True
}

# Function to show table data
def show_table(cursor, table_name):
    print(f"\n-- Displaying {table_name} Table --\n")
    cursor.execute(f"SELECT * FROM {table_name}")
    records = cursor.fetchall()
    for record in records:
        print(record)

try:
    # Connect to database
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    # List of tables to display
    tables = [
        "Employee",
        "Work_Log",
        "Supplier",
        "Supply_Item",
        "Wine",
        "Orders",
        "Status",
        "Distributor",
        "Shipment"
    ]

    # Loop through and display each table
    for table in tables:
        show_table(cursor, table)

    input("\n\nPress any key to continue...")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("The supplied username or password are invalid")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("The specified database does not exist")
    else:
        print(err)

finally:
    if db.is_connected():
        db.close()