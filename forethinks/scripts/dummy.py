import pyodbc
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# Database connection details
server = '192.168.29.237'
database = 'ForeThinks'
username = 'medha'
password = 'medha@feb2023'

# Connect to the database
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

# Query the view
query = "SELECT * FROM Daily_CPA_MinMax_Obeyed ORDER BY EXPIRY_DT, TIMESTAMP"
data = pd.read_sql(query, conn)

# Create a new Excel workbook
wb = Workbook()
ws = wb.active

# Loop through the unique values of EXPIRY_DT
for expiry in data['EXPIRY_DT'].unique():
    # Filter the data for the current EXPIRY_DT value
    filtered_data = data[data['EXPIRY_DT'] == expiry]
    
    # Create a new sheet in the workbook with the name of the current EXPIRY_DT value
    ws = wb.create_sheet(str(expiry))
    
    # Write the filtered data to the sheet
    for row in dataframe_to_rows(filtered_data, index=False, header=True):
        ws.append(row)

# Save the workbook to the specified directory
wb.save(r'D:\\ForeThinks\\server\\forethinks\\output.xlsx')