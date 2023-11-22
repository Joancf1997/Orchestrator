# Orchestrator class
# This object orchestrates the execution flow of the steps (functions)
# % conda create -n "Orchestrator" python=3.11.5 
import json
from os import path
from Core.Helper import Helper



class Orchestrator(): 
    def __init__(self, steps):
        self.steps = steps                                  # List of steps to execute 
        self.globalConfigFilePath = "./Config/General.json" # Path to global configuration file
        self.sqlConfinFilePath    = "./Config/Queries.json" # Path to the SQL configuration file
        self.sqlFolderPath        = "./SQL"                 # Path to the SQL configuration file
        self.globalConfig = None                            # Global configuration parameters
        self.sqlQueriesConfig = None                        # Global configuration parameters
        
        # Initial validations
        self.global_setup_config()                          # Apply the global configurations
        self.queries_config()                               # Validate SQL configuration 
        self.helper = Helper(self.globalConfig["DB"])       # Inicialize helper class
        self.helper.DB_connect()                            # Connect to DB



    # Read global configuration file parameters 
    def queries_config(self): 
        # Validate if config file exist
        if path.isfile(self.sqlConfinFilePath):
            with open(self.sqlConfinFilePath, 'r') as file:
                self.sqlQueriesConfig = json.load(file)

            # Validate sql configuration and files for every step
            # Every step can have a carpet and every carpet has sql files, they have to be declared on the SQL config file in order to be executed during the step

            for step in self.steps: 
                folderName = step.__name__
                folderPath = path.join(self.sqlFolderPath, folderName)

                # check if the Step has SQL files declared
                if folderName in self.sqlQueriesConfig.keys(): 

                    # Check if the folder exist
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



    # Execute all the steps in order
    def execute(self): 
        print("-------------------Statirng Orchestrator execution------------------- \n")
        # Steps Execution
        try:
            for func in self.steps:
                print("\n --------- STEP " + func.__name__ + " --------- ")

                # Execute queries of the current step if it has 
                print("1.  Queries: ")
                if func.__name__ in self.sqlQueriesConfig.keys(): 
                    sqlFiles = self.sqlQueriesConfig[func.__name__]["files"]
                    for file in sqlFiles: 
                        # Read file
                        filePath = path.join(self.sqlFolderPath, func.__name__, file)
                        with open(filePath, 'r') as sql_file:
                            sql_queries = sql_file.read()
                            self.helper.execute_file_query(file, sql_queries)
                    
                # Execute the code of the step
                func()
        except Exception as e:
            print(e)
        else:
            print("All functions executed successfully!!")
        self.helper.DB_disconnect()
        print("-------------------Execution completed-------------------")

    
    # Show the list of steps that will be followed during the execution
    def list_steps(self):
        print("List of steps ")
        print(self.steps)

    # Send the instance of the helper
    def get_hepler(self): 
        return self.helper