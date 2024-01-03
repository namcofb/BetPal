import sqlite3

import pandas as pd


class SQLRepository():
    '''
    SQL repository class: Wrangles raw data into a dataframe/
                          Inserts data into sqldatabse/
                          Reads data from database and returns a dataframe.

    Argument(s)
    ----------
        connection: sqlite database connection                
    '''
    def __init__(self, connection):

        self.connection = sqlite3.connect(connection, check_same_thread=False)


    def wrangle_func(self, filepath):
        '''
        Wrangle_func: Function to clean raw data. Saves result to attribute `self.data`.

        Parameter(s)
        ------------
            filepath: str
                The location of file to be wrangled
        '''
        # Create dictionary to hold the needed information
        info_dict = {
            "Date": [],
            "Status": [],
            "Total Stake": [],
            "Total Return": []
        }
        # Use file handling to open and read txt file
        with open(filepath) as f:
            first_line = f.readlines()
        # Use filter and map funtion to further clean data
        first_line_no_break = list(filter(lambda x: False if x == "\n" else True, first_line))
        first_line_no_break = list(map(lambda x: x.replace("\n", "") if "\n" in x else x, first_line_no_break))

        # Create for loop to append cleaned data into info_dict
        for line in first_line_no_break:
            if "/" in line:
                info_dict["Date"].append(line.split(" ")[0])
            if "Multiple" in line:
                info_dict["Status"].append(line.replace("Multiple", ""))
            if "Singles" in line:
                info_dict["Status"].append(line.replace("Singles", ""))
            if "Total Stake" in line:
                info_dict["Total Stake"].append(line.replace("Total Stake:", ""))
            if "Total Return" in line:
                info_dict["Total Return"].append(line.replace("Total Return:", ""))

        # Use pandas to create dataframe as df
        df = pd.DataFrame(info_dict)
        
        df["Total Stake"] = df["Total Stake"].str.replace(",", "").astype(float)
        
        df["Total Return"] = df["Total Return"].str.replace(",", "").replace("--", "0.00").astype(float)
        
        # Set index to `Date` and convert to DatetimeIndex
        df.set_index("Date", inplace=True)
        df.index = pd.to_datetime(df.index, format="%d/%m/%Y")
        # Assign dataframe to `self.data`
        self.data = df

    def insert_table(self, table_name, if_exists="fail"):
        '''
        Insert_table: Writes pandas dataframe into sqlite database.

        Parameters
        ----------
            table_name: str
                Name assign to new table in db.
            if_exists: deafault "fail": Raise a ValueError.
                       "replace": Drop the table before inserting new values.
                       "append": Insert new values to the existing table.
        '''
        # Insert new table into db using pandas
        try:
            row_entries = self.data.to_sql(name=table_name, con=self.connection, if_exists=if_exists)
            # Return status dictionary
            return {
                "Success": True,
                "Rows Added": row_entries
            }
        except Exception as e:
            return str(e)


    def read_table(self, table_name, merge=False):
        '''
        Read_table: Reads table from sqlite db to pandas dataframe.

        Parameters
        ----------
            table_name: str
                Table to be pulled from db.
            merge: bool
                Merges all tables in db into one pandas dataframe
        '''
        # Starter code if merge is set to True
        # Using cursor attr to get list
        # cursor = self.connection.cursor()
        # cusrsor.execute("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name").fetchall()
        # OR
        # To get a list of tables in the db
        db_tables = pd.read_sql_query(sql="SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name", con=self.connection)
        if len(db_tables) > 1:
            upd_tables = db_tables.squeeze().tolist()
        else:
            upd_tables = db_tables.squeeze()
        # Assign list to `tables`
        # Create a query string 
        if merge:
            count = 0
            query = ""
            # Looping through each table in db
            for table in upd_tables:
                count += 1
                if count == len(upd_tables):
                    query += f"SELECT * FROM {table}"
                else:
                    query += f"SELECT * FROM {table} UNION ALL "
                    
            df = pd.read_sql(sql=query, con=self.connection, index_col="Date", parse_dates=True)
            df.index = pd.DatetimeIndex(df.index)
        else:
            df = pd.read_sql(sql=f"SELECT * FROM {table_name}", con=self.connection, index_col="Date", parse_dates=True)
            df.index = pd.DatetimeIndex(df.index)

        # Return dataframe
        return df
    



