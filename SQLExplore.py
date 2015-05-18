import psycopg2
import pandas as pd

CONN = psycopg2.connect(database="cbb", user="seth", password="abc123",
                        host="localhost", port="5432")
CUR = CONN.cursor()

q = {}

q['missing_games'] = """SELECT 
                            COUNT(dt) AS missing_games, 
                            EXTRACT(YEAR FROM dt) AS SEASON
                        FROM games_ncaa
                        WHERE game_id NOT IN
                            (SELECT DISTINCT(game_id) FROM ncaa_pbp)
                        GROUP BY EXTRACT(YEAR FROM dt)
                        ORDER BY EXTRACT(YEAR FROM dt)
                     """
q['foul_totals'] = """SELECT game_id, play, teamid, time,
                        SUM(CASE WHEN play='FOUL' AND teamid=1 THEN 1
                        ELSE 0
                        END) OVER (PARTITION BY game_id ORDER BY time) AS home_foul
                      FROM ncaa_pbp
                      LIMIT 50;
                   """
q['foul_totals'] = """SELECT game_id, play, teamid, time,
                        CASE WHEN play='FOUL' AND teamid=1 THEN 1
                        ELSE 0
                        END home_foul
                      FROM ncaa_pbp
                      ORDER BY game_id, time
                      LIMIT 50;
                   """
if __name__ == '__main__':
    # df = pd.read_sql(q['missing_games'], CONN)
    # print df
    CUR.execute("""SELECT * FROM ncaa_pbp""", CONN)
    for result in CUR.fetchall():
        x = result