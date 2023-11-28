import mysql.connector as mysql
import csv
from datetime import datetime

# Connect to the DB server and database
conn = mysql.connect(user='root', password='akshay', host='localhost', database='pysparkDB', port=3307)
cursor = conn.cursor()

# Open the CSV file
with open('database.csv', mode='r') as csv_file:
    # Read CSV using the reader class
    csv_reader = csv.reader(csv_file)
    
    # Skip header
    header = next(csv_reader)
    
    # Read CSV row-wise and insert into the table
    for row in csv_reader:
        # Convert the date format from dd-mm-yyyy to yyyy-mm-dd
        date_str = row[0]  # Assuming the date is in the first column
        converted_date = datetime.strptime(date_str, "%d-%m-%Y").strftime("%Y-%m-%d")
        
        # Update the date in the row
        row[0] = converted_date

        # Handle null values for all columns
        for col_index, col_value in enumerate(row):
            if col_value is None or col_value == '':
                row[col_index] = None  # Replace null values with None

        # Define your SQL query with placeholders for all columns
        sql = """
        INSERT INTO EarthquakeData (
            Date, Time, Latitude, Longitude, Type, Depth, Depth_Error, Depth_Seismic_Stations,
            Magnitude, Magnitude_Type, Magnitude_Error, Magnitude_Seismic_Stations,
            Azimuthal_Gap, Horizontal_Distance, Horizontal_Error, Root_Mean_Square,
            ID, Source, Location_Source, Magnitude_Source, Status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Execute the query with the row values
        cursor.execute(sql, tuple(row))
        print("Record inserted")

# Commit the changes and close the cursor
conn.commit()
cursor.close()
