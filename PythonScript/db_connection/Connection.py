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
        # Try to connect database
        print(self.host, self.user, self.password, self.database)
        try:
            with connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database
            ) as connection:
                print(connection)
        except Error as e:
            print(e)

        return connection
