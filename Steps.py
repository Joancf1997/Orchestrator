# List of steps to be exetuted 

class Steps(): 
    def __init__(self):
        self.payload = None
        self.helper = None

    def Extract(self):
        pass
        

    def Transform(self):
        pass


    def Load(self):
        # Your implementation for the third function
        pass

    def work(self):
        alerts = self.helper.execute_sql_select("SQL/Individual/alerts.sql")
        self.helper.insert_dataframe(alerts[0:7], "alerts2")

    def Drop(self): 
        pass
    



