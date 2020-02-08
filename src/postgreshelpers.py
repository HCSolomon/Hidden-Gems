import psycopg2

class PostgreSQLConnector:
    def __init__(self, 
                user="postgres", 
                password="", 
                host="localhost", 
                port="5432", 
                database="hiddengems_db"):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database