import pyodbc
import pandas as pd
import os
import subprocess
import io
import logging
import sys
import time

# Set up logging
logging.basicConfig(filename='D:\\ForeThinks\\server\\forethinks\\logs\\critical_log', level=logging.CRITICAL)

server = '192.168.29.237'
database = 'ForeThinks'
username = 'medha'
password = 'medha@feb2023'
driver = '{ODBC Driver 17 for SQL Server}'
csv_directory = 'D:\\ForeThinks\\server\\forethinks\\temp_data'
data_directory = 'D:\\ForeThinks\\server\\forethinks\\temp_data\\data_frame_data'

connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Connect to the SQL Server database
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Get a list of all the CSV files in the directory
csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]

# Loop through the CSV files
for csv_file in csv_files:
    try:
        print(f"started processing file : {csv_file}")
        # Read the CSV file into a DataFrame
        df = pd.read_csv(os.path.join(csv_directory, csv_file))
        # Print the contents of the DataFrame
        #print(df)
        # Convert the EXPIRY_DT and TIMESTAMP columns to the desired format
        df['EXPIRY_DT'] = pd.to_datetime(df['EXPIRY_DT'], format='%d-%b-%Y').dt.strftime('%Y-%m-%d')
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='%d-%b-%Y').dt.strftime('%Y-%m-%d')
        df['STRIKE_PR'] = df['STRIKE_PR'].astype(float)
        df['OPEN'] = df['OPEN'].astype(float)
        df['HIGH'] = df['HIGH'].astype(float)
        df['LOW'] = df['LOW'].astype(float)
        df['CLOSE'] = df['CLOSE'].astype(float)
        df['SETTLE_PR'] = df['SETTLE_PR'].astype(float)
        df['CONTRACTS'] = df['CONTRACTS'].astype(float)
        df['VAL_INLAKH'] = df['VAL_INLAKH'].astype(float)
        df['OPEN_INT'] = df['OPEN_INT'].astype(float)
        df['CHG_IN_OI'] = df['CHG_IN_OI'].astype(float)
        
        # Drop any columns with 'Unnamed' in their name
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        # Remove rows with empty values in the SYMBOL column
        df = df[df['SYMBOL'].notna()]
        # Add an ID column to the DataFrame
        df['ID'] = range(1, len(df) + 1)
        # Print the contents of the DataFrame
        #print(df)
        # Get the first value of the TIMESTAMP column
        first_timestamp = df['TIMESTAMP'].iloc[0]

        cursor.execute("SELECT COUNT(*) FROM RawData WHERE TIMESTAMP=?", (first_timestamp,))
        count = cursor.fetchone()[0]
        if count == 0:
            # Extract the day, month, year, and day of the week
            day = pd.to_datetime(df['TIMESTAMP'])
            actualDay = day.dt.day.iloc[0]
            month = day.dt.strftime('%b').iloc[0]
            year = day.dt.year.iloc[0]
            day_of_week = day.dt.strftime('%A').iloc[0]

            # Write the data to a CSV file
            data_file = os.path.join(data_directory, f'{csv_file}.data')
            if not os.path.isfile(data_file):
                df.to_csv(data_file, index=False, header=False)
            else:
                print(f"data file for file : {csv_file} already exists hence skipping write operation")
            num_rows = df.shape[0]

            # Call the bcp utility to import the data into the RawData table
            bcp_command = f'bcp RawData in {data_file} -c -t, -S {server} -d {database} -U {username} -P {password} -b 100000 -a 32767'
            subprocess.run(bcp_command, shell=True, check=True)
            print(f"successfully inserted data for file : {csv_file}")

            # Count the number of records in RawData table where TIMESTAMP = first_timestamp
            cursor.execute("SELECT COUNT(*) FROM RawData WHERE TIMESTAMP=?", (first_timestamp,))
            totalRecords = cursor.fetchone()[0]
            print(f"total records for this date : {totalRecords}")

            cursor.execute("SELECT * FROM BhavCopyDownloadAudit WHERE DATE=?", (first_timestamp,))
            row = cursor.fetchone()
            if row:
                cursor.execute("UPDATE BhavCopyDownloadAudit SET INSERT_INTO_RAWDATA_STATUS=?, TotalRecords=?   WHERE DATE=?", (1, totalRecords, first_timestamp))
            else:
                cursor.execute("INSERT INTO BhavCopyDownloadAudit ([DATE],[DOWNLOAD_STATUS],[INSERT_INTO_RAWDATA_STATUS],[DAY_NUMBER],[MONTH],[YEAR],[DAY_OF_THE_WEEK],[TotalRecords],[VERIFIED_STATUS]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", first_timestamp, 1, 1, int(actualDay), month, int(year), day_of_week, totalRecords, 0 )
            print(f"successfully inserted audit for file : {csv_file}")
            # Commit the transaction
            conn.commit()
        else:
            print("data for this date already exists in RawData table")
    except Exception as e:
        print(f"error occured while inserting data for file {csv_file} : {e}")
        # Roll back the transaction
        retries = 3
        while(retries>0) :
            if isinstance(e, ConnectionError):
                time.sleep(180)  # Wait for 3 minutes
            try:
                conn.rollback()
                cursor.execute("SELECT * FROM BhavCopyDownloadAudit WHERE DATE=?", (first_timestamp,))
                row = cursor.fetchone()
                if not row :
                    cursor.execute("INSERT INTO BhavCopyDownloadAudit ([DATE],[DOWNLOAD_STATUS],[INSERT_INTO_RAWDATA_STATUS],[DAY_NUMBER],[MONTH],[YEAR],[DAY_OF_THE_WEEK],[TotalRecords],[VERIFIED_STATUS]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", first_timestamp, 1, 0, int(actualDay), month, int(year), day_of_week, 0, 0 )
                print(f"file insertion for file : {csv_file} failed, successfully inserted audit")
                print(f"An error occurred while processing file {csv_file}: {e}")
                break
            except Exception as error:
                e = error  # Assign the value of the second exception to e
                if retries == 1 :
                    logging.critical(f'transaction for file : {csv_file} could not be rolled back,file being copied to rollback failed files')
                    # Move the file to the rollback_failed_files directory
                    src = os.path.join(csv_directory, csv_file)
                    dst = os.path.join('D:\\ForeThinks\\server\\forethinks\\temp_data\\rollback_failed_files', csv_file)
                    os.rename(src, dst)
                    sys.exit(1)
                else :
                    retries-=1


# Close the cursor and connection
cursor.close()
conn.close()