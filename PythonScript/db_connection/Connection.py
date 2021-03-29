import os

from dotenv import load_dotenv, find_dotenv
from mysql.connector import connect, Error


class Connection:
    def __init__(self):
        load_dotenv(find_dotenv())
        self.host = os.getenv("DB_HOST")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.database = os.getenv("DB_DATABASE")

    def connect(self):

        config = {
            'user': 'root',
            'password': '',
            'host': 'localhost',
            'port': '3306',
            'database': 'brain_station',
            'raise_on_warnings': True,
        }

        # Try to connect database
        print(self.host, self.user, self.password, self.database)
        try:
            with connect(**config) as connection:
                print(connection)
        except Error as e:
            print("Error Heppend!")
            print(e)

        return connection
