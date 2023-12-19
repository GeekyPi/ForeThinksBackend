import pyodbc
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# Database connection details
server = '192.168.29.237'
database = 'ForeThinks'
username = 'medha'
password = 'medha@feb2023'

# Create a connection to the database
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

# Create a cursor to execute queries
cursor = conn.cursor()

# Get all unique Put_TIMESTAMP values from the view
cursor.execute("SELECT DISTINCT Put_TIMESTAMP FROM dbo.BNF_RawCommon")
timestamps = cursor.fetchall()

# Iterate over each Put_TIMESTAMP value
for timestamp in timestamps:
    # Convert the timestamp to the desired format for the file name
    timestamp_str = timestamp[0].strftime('%Y_%m_%d')
    # Create a new Excel file for this timestamp
    wb = Workbook()
    # Get all unique Put_EXPIRY_DT values for this timestamp
    cursor.execute("SELECT DISTINCT Put_EXPIRY_DT FROM dbo.BNF_RawCommon WHERE Put_TIMESTAMP=?", timestamp[0])
    expiry_dates = cursor.fetchall()
    # Iterate over each Put_EXPIRY_DT value
    for expiry_date in expiry_dates:
        # Convert the expiry date to the desired format for the sheet name
        expiry_date_str = expiry_date[0].strftime('%Y_%m_%d')
        # Create a new sheet for this expiry date
        ws = wb.create_sheet(f'common_{expiry_date_str}')
        # Get all data from the view for this timestamp and expiry date
        data = pd.read_sql(f"SELECT * FROM dbo.BNF_RawCommon WHERE Put_TIMESTAMP='{timestamp[0]}' AND Put_EXPIRY_DT='{expiry_date[0]}'", conn)
        # Write the data to the sheet
        for row in dataframe_to_rows(data, index=False, header=True):
            ws.append(row)
    # Save the Excel file
    wb.save(f'D:\\AnlaysisSheets\\Data_{timestamp_str}.xlsx')

# Close the database connection
conn.close()