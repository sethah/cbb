import psycopg2
import pandas as pd

CONN = psycopg2.connect(database="cbb", user="seth", password="abc123",
                        host="localhost", port="5432")
CUR = CONN.cursor()

class CBBQuery(object):

    def __init__(self):
        pass

    def streak(self, n=5):
        q = """ SELECT 
                    season,
                    daynum, 
                    team,
                    opp_team,
                    score,
                    opp_score,
                    outcome,
                    COALESCE(SUM(CAST(outcome AS INT))
                             OVER(
                                PARTITION BY (team, season)
                                ORDER BY daynum
                                ROWS BETWEEN {n} PRECEDING
                                AND 1 PRECEDING
                             ) / CAST({n} AS REAL), 0.5) AS streak{n}
                FROM games_unstacked
                ORDER BY daynum
            """.format(n=n)
        return q
if __name__ == '__main__':
    c = CBBQuery()
    df = pd.read_sql(c.streak(), CONN)
    print df.head(50)