
# Helpe class to interact with the database
from Core.DB import DB
from os import path


class Helper():
    def __init__(self, dbConfig):
        self.databaseConf = None                            # Condiguration paramaters to connect to the DB
        self.db = None                                      # Database instance
        self.databaseConf = dbConfig                        # Database configuration
    
    
    # Create a connection with a database 
    def DB_connect(self):
        # self.databaseConf = self.globalConfig["DB"]
        self.db = DB(
            self.databaseConf["database"],
            self.databaseConf["host"],
            self.databaseConf["user"],
            self.databaseConf["password"],
            self.databaseConf["port"]
        )      
        self.db.connect()

    # Create a connection with a database 
    def DB_disconnect(self):
        self.db.disconect()


    def execute_file_query(self, file, sql_queries):
        self.db.execute_file_query(file, sql_queries)


    # Insert a dataframe using a query file
    def insert_dataframe(self, dataframe, table):
        insert_query = f"INSERT INTO {table} ({', '.join(dataframe.columns)}) VALUES ({', '.join(['%s']*len(dataframe.columns))})"
        data_values = [tuple(row) for row in dataframe.values]
        return self.db.execute_sql_insert(insert_query, data_values)
    

    # Load a dataframe form a select query file
    def execute_sql_select(self, filePath):
        # Validate if file exist 
        if path.exists(filePath):
            with open(filePath, 'r') as sql_file:
                query = sql_file.read()
                return self.db.execute_sql_select(query)
        return None