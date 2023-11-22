# Orchestrator class
# This object orchestrates the execution flow of the steps (functions)
# % conda create -n "Orchestrator" python=3.11.5 
from DB import DB
import json
from os import path



class Orchestrator(): 
    def __init__(self, steps):
        self.steps = steps                                  # List of steps to execute 
        self.globalConfigFilePath = "./Config.json"         # Path to global configuration file
        self.sqlConfinFilePath    = "./Queries.json"        # Path to the SQL configuration file
        self.sqlFolderPath        = "./SQL"                 # Path to the SQL configuration file
        self.globalConfig = None                            # Global configuration parameters
        self.sqlQueriesConfig = None                        # Global configuration parameters
        self.databaseConf = None                            # Condiguration paramaters to connect to the DB
        
        # Initial validations
        self.global_setup_config()                          # Apply the global configurations
        self.queries_config()                               # Validate SQL configuration 



    # Read global configuration file parameters 
    def queries_config(self): 
        # Validate if config file exist
        if path.isfile(self.sqlConfinFilePath):
            with open(self.sqlConfinFilePath, 'r') as file:
                self.sqlQueriesConfig = json.load(file)

            # Validate sql configuration and files for every step
            # Every step has a carpet and every carpet has sql files, they have to be declared on the SQL config file
            # Validate that every steps has a query section and all folders and files exist
            sqlFileFolders = self.sqlQueriesConfig.keys()

            for step in self.steps: 
                folderName = step.__name__
                folderPath = path.join(self.sqlFolderPath, folderName)

                # Check if the step is declared on the SQL config file.
                if folderName in sqlFileFolders:

                    # Check if folder path exist
                    if path.exists(folderPath):
                        
                        # Validate if files exist
                        files = self.sqlQueriesConfig[folderName]["files"]
                        for file in files: 
                            filePath = path.join(folderPath, file)

                            if not path.exists(filePath):
                                raise Exception("ERROR: File " + file + " does not exist in folder " + folderName + "!!")
                    else: 
                        raise Exception("ERROR: The folder of the step " + folderName + " does not exist!!")
                else: 
                    raise Exception("ERROR: The setp " + folderName + " is not declare on the SQL config file!!")
        else: 
            raise Exception("ERROR: SQL configuration file missing!!")
        print("Configuración de SQL exitosa!!")




        
    # Read global configuration file parameters 
    def global_setup_config(self): 
        # Validate if config file exist
        if path.isfile(self.globalConfigFilePath):
            with open(self.globalConfigFilePath, 'r') as file:
                self.globalConfig = json.load(file)

            # Validate db parameters
            if "DB" not in self.globalConfig:
                raise Exception("ERROR: DB parameters missing on global configuration file!!")
        else: 
            raise Exception("ERROR: Global configuration file missing!!")

        # Connect to database
        self.DB_connect()





    # Create a connection with a database 
    def DB_connect(self):
        self.databaseConf = self.globalConfig["DB"]
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




    # Execute all the steps in order
    def execute(self): 
        print("Statirng Orchestrator execution...")
        # Steps Execution
        try:
            for func in self.steps:
                print("---------------STEP " + func.__name__ + "---------------")

                # Execute queries of the current step 
                sqlFiles = self.sqlQueriesConfig[func.__name__]["files"]
                for file in sqlFiles: 
                    # Read file
                    filePath = path.join(self.sqlFolderPath, func.__name__, file)
                    with open(filePath, 'r') as sql_file:
                        sql_queries = sql_file.read()
                        self.db.execute_file_query(file, sql_queries)
                    
                # Execute the code of the step
                func()
        except Exception as e:
            print(e)
        else:
            print("All functions executed successfully.")
        self.DB_disconnect
        print("Execution completed..")



    
    # Show the list of steps that will be followed during the execution
    def list_steps(self):
        print("List of steps ")
        print(self.steps)
