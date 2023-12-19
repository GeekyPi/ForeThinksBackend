import pyodbc
import pandas as pd
from math import floor
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

try:
    # Database connection details
    server = '192.168.29.237'
    database = 'ForeThinks'
    username = 'medha'
    password = 'medha@feb2023'

    # Connect to the database
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

    # Create a new Excel file called OnDemandData
    file_name = 'D:\\AnlaysisSheets\\OnDemandData.xlsx'
    wb = Workbook()

    # Create a new sheet in the Excel file
    ws = wb.create_sheet('Data')

    # Query the database for unique Put_TIMESTAMP values
    timestamps = pd.read_sql("SELECT DISTINCT Put_TIMESTAMP FROM BNF_RawCommon", conn)

    # Add a variable to keep track of whether it's the first iteration of the loop
    first_iteration = True

    # Loop through each unique Put_TIMESTAMP value
    for index, row in timestamps.iterrows():
        put_timestamp = row['Put_TIMESTAMP']
        put_timestamp_str = put_timestamp.strftime('%Y_%m_%d')
        
        # Query the BNF_Index table for the OPEN value for this Put_TIMESTAMP value
        open_value = pd.read_sql(f"SELECT [OPEN] FROM BNF_Index WHERE HistoricalDate = '{put_timestamp}'", conn).iloc[0]['OPEN']
        
        # Calculate the range of values to search for
        range_start = floor(open_value / 100) * 100
        range_end = range_start + 100
        
        # Query the database for the minimum Put_EXPIRY_DT value for this Put_TIMESTAMP value
        put_expiry_dt = pd.read_sql(f"SELECT MIN(Put_EXPIRY_DT) as Put_EXPIRY_DT FROM BNF_RawCommon WHERE Put_TIMESTAMP = '{put_timestamp}'", conn).iloc[0]['Put_EXPIRY_DT']
        put_expiry_dt_str = put_expiry_dt.strftime('%Y_%m_%d')
        
        # Query the database for data that falls within the specified range for this Put_TIMESTAMP and minimum Put_EXPIRY_DT value
        data = pd.read_sql(f"""
            SELECT BNF_RawCommon.Put_TIMESTAMP as TIMESTAMP, BNF_RawCommon.Put_EXPIRY_DT as EXPIRY_DT,
            BNF_RawCommon.Put_STRIKE_PR as STRIKE_PR, BNF_RawCommon.Put_OPEN, BNF_RawCommon.Put_HIGH,
            BNF_RawCommon.Put_LOW, BNF_RawCommon.Put_CLOSE, BNF_RawCommon.Call_OPEN, BNF_RawCommon.Call_HIGH,
            BNF_RawCommon.Call_LOW, BNF_RawCommon.Call_CLOSE, BNF_RawCommon.CallPlus, BNF_RawCommon.CallMinus,
            BNF_RawCommon.PutPlus, BNF_RawCommon.PutMinus, BNF_RawCommon.CMPP_AVG, BNF_RawCommon.CPPM_AVG,
            BNF_RawCommon.CPPP_AVG, BNF_RawCommon.CMPM_AVG,
            BNF_Index.[OPEN], BNF_Index.[HIGH], BNF_Index.[LOW], BNF_Index.[CLOSE]
            FROM BNF_RawCommon
            JOIN BNF_Index ON BNF_RawCommon.Put_TIMESTAMP = BNF_Index.HistoricalDate
            WHERE Put_TIMESTAMP = '{put_timestamp}' AND Put_EXPIRY_DT = '{put_expiry_dt}' AND (
            CMPP_AVG BETWEEN {range_start} AND {range_end} OR 
            CPPM_AVG BETWEEN {range_start} AND {range_end} OR 
            CPPP_AVG BETWEEN {range_start} AND {range_end} OR 
            CMPM_AVG BETWEEN {range_start} AND {range_end}
            )
            """, conn)
        
        # Write the data to the sheet in the Excel file
        for r in dataframe_to_rows(data, index=False, header=first_iteration):
            ws.append(r)

        # Set first_iteration to False after the first iteration of the loop
        first_iteration = False

    # Save and close the Excel file
    wb.save(file_name)
except Exception as e:
    print(f'An error occurred: {e}')