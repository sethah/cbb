from DataCollection.DBScrapeUtils import *
import psycopg2

CONN = psycopg2.connect(database="cbb", user="sethhendrickson",
                        password="abc123", host="localhost", port="5432")
CUR = CONN.cursor()

def test_games_to_scrape():
    games = get_games_to_scrape(2012, 'box', 10)
    assert len(games) == 10

    games = get_games_to_scrape(1982, 'box', 10)
    assert games == []