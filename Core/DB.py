# Class to administrate the conection with a PostgresSQL Database
import psycopg2    # pip3 install psycopg2-binary / https://pypi.org/project/psycopg2-binary/
from psycopg2 import extras
import pandas as pd


# Class to interact with the database
class DB():
    def __init__(self, db="", host="", user="", passw="", port=""):
        #Define the parameters to connect to the DB 
        self.conn = None
        self.cursor = None

        # Database configuration parameters
        self.db = db
        self.host = host
        self.user = user
        self.passw = passw
        self.port = port



    # Connecto to the DB 
    def connect(self): 
        try:
            print("Connecting to database...")
            self.conn = psycopg2.connect(
                database=self.db, 
                host=self.host,
                user=self.user, 
                password=self.passw, 
                port=self.port
            )
            self.cursor = self.conn.cursor(cursor_factory=extras.DictCursor)
            print("Database connected successfully!!")
            return self
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise Exception("ERROR: Database connection error!!")
            

    # Disconnect from the DB 
    def disconect(self):
        print("Closing database connection...")
        if None not in [self.conn, self.cursor]:
            self.cursor.close()
            self.conn.close()
            print('Database connection closed!!')
        else: 
            print('Database is not connected!!')


    # Execute the query loaded from a file .sql
    def execute_file_query(self, fileName, query): 
        print("Executing File " + fileName)
        try:
            self.cursor.execute(query)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise Exception("ERROR: Executing the file " + fileName)
        

    # Function to insert a dataframe into a table
    def execute_sql_insert(self, query, values): 
        self.cursor.executemany(query, values)
        return self.conn.commit()



    # Function to execute a select query and return the result
    def execute_sql_select(self, query): 
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return pd.DataFrame(data, columns = columns)