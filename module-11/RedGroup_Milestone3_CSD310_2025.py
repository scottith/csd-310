# Red Group
# Dario Gomez, Juedeja Richard, Kristopher Kuenning, Scott Macioce

import mysql.connector
from mysql.connector import errorcode
from dotenv import dotenv_values
from tabulate import tabulate
from datetime import datetime


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
def print_table(headers, data, title):
    print(f"\n=== {title} ===")
    print(tabulate(data, headers=headers, tablefmt="grid"))


try:
    # Connect to database
    db = mysql.connector.connect(**config)
    cursor = db.cursor()


    try:
        query1 = """
        SELECT 
            Supplier.Supplier_ID,
            Supplier.Supplier_Name,
            EXTRACT(YEAR FROM Shipment.Expected_Delivery_Date) AS Year,
            EXTRACT(MONTH FROM Shipment.Expected_Delivery_Date) AS Month,
            AVG(Shipment.Date_Delta) AS Avg_Delivery_Delay,
            COUNT(CASE 
                    WHEN Shipment.Date_Delta > 0 
                    THEN 1 
                    END) AS Late_Shipments,
            COUNT(*) AS Total_Shipments
        FROM Shipment
        JOIN Supplier ON Shipment.Supplier_ID = Supplier.Supplier_ID
        GROUP BY 
            Supplier.Supplier_ID, 
            Supplier.Supplier_Name, 
            EXTRACT(YEAR FROM Shipment.Expected_Delivery_Date), 
            EXTRACT(MONTH FROM Shipment.Expected_Delivery_Date)
        ORDER BY 
            Year, Month, Avg_Delivery_Delay DESC;
        """
        cursor.execute(query1)
        results1 = cursor.fetchall()
        headers1 = ["Supplier ID", "Supplier Name", "Year", "Month", "Avg Delivery Delay (days)", "Late Shipments", "Total Shipments"]
        print(f"\n[Report Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
        print_table(headers1, results1, "Supplier Delivery Performance (Month-by-Month)")
    except mysql.connector.Error as err:
        print(f"Error executing Query 1: {err}")
    finally:
        pass

# Query 2a: Wine Sales Analysis
    try:
        query2a = """
        SELECT 
            Wine.Wine_ID,
            Wine.Wine_Type,
            Wine.Stock_Quantity AS Expected_Stock,
            COALESCE(SUM(Monthly_Sales.Bottles_Sold), 0) AS Total_Sold,
            (COALESCE(SUM(Monthly_Sales.Bottles_Sold), 0) * 100.0 / Wine.Stock_Quantity) AS Sell_Through_Rate
        FROM Wine
        LEFT JOIN Monthly_Sales ON Wine.Wine_ID = Monthly_Sales.Wine_ID
        GROUP BY Wine.Wine_ID, Wine.Wine_Type, Wine.Stock_Quantity
        ORDER BY Sell_Through_Rate ASC;
        """
        cursor.execute(query2a)
        results2a = cursor.fetchall()
        headers2a = ["Wine ID", "Wine Type", "Expected Stock", "Total Sold", "Sell-Through Rate (%)"]
        print(f"\n[Report Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
        print_table(headers2a, results2a, "Wine Sales Analysis")
    except mysql.connector.Error as err:
        print(f"Error executing Query 2a: {err}")
    finally:
        pass

# Query 2b: Distributor-Wine Mapping
    try:
        query2b = """
        SELECT DISTINCT 
            Distributor.Distributor_ID,
            Distributor.Distributor_Name,
            Wine.Wine_ID,
            Wine.Wine_Type
        FROM Distributor
        JOIN Orders ON Distributor.Distributor_ID = Orders.Distributor_ID
        JOIN Wine ON Orders.Wine_ID = Wine.Wine_ID
        ORDER BY Distributor.Distributor_ID, Wine.Wine_ID;
        """
        cursor.execute(query2b)
        results2b = cursor.fetchall()
        headers2b = ["Distributor ID", "Distributor Name", "Wine ID", "Wine Type"]
        print(f"\n[Report Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
        print_table(headers2b, results2b, "Distributor-Wine Mapping")
    except mysql.connector.Error as err:
        print(f"Error executing Query 2b: {err}")
    finally:
        pass

# Query 3: Employee Hours (Last Four Quarters)
    try:
        query3 = """
        SELECT 
            Employee.Employee_ID,
            Employee.First_Name,
            Employee.Last_Name,
            CONCAT('Q', Work_Log.Quarter, ' 2025') AS Quarter,
            SUM(Work_Log.Hours_Worked) AS Total_Hours
        FROM Employee
        JOIN Work_Log ON Employee.Employee_ID = Work_Log.Employee_ID
        WHERE Work_Log.Date BETWEEN '2024-04-01' AND '2025-03-31'
        GROUP BY Employee.Employee_ID, Employee.First_Name, Employee.Last_Name, Work_Log.Quarter
        ORDER BY Employee.Employee_ID, Work_Log.Quarter;
        """
        cursor.execute(query3)
        results3 = cursor.fetchall()
        headers3 = ["Employee ID", "First Name", "Last Name", "Quarter", "Total Hours"]
        print(f"\n[Report Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
        print_table(headers3, results3, "Employee Hours (Last Four Quarters)")
    except mysql.connector.Error as err:
        print(f"Error executing Query 3: {err}")
    finally:
        pass


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