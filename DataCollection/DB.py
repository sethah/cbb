import psycopg2

database = "cbb"
user = "sethhendrickson"
password = "abc123"
host = "localhost"
port = "5432"
conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
TABLES = {"games": "games_test",
          "box": "box_stats",
          "pbp": "pbp_stats",
          "raw_pbp": "raw_pbp",
          "division_one": "division_one",
          "teams": "teams",
          "kenpom_ranks": "kenpom_ranks"}
