import psycopg2

class DB(object):
    conn = psycopg2.connect(database="cbb", user="sethhendrickson",
                            password="abc123", host="localhost", port="5432")
