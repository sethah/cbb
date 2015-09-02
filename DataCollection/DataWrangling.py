import pandas as pd
import psycopg2

conn = psycopg2.connect(database="cbb", user="seth", password="abc123",
                        host="localhost", port="5432")
cur = conn.cursor()

def add_covers():
    df = pd.read_csv('covers.csv')
    df = df.drop('Unnamed: 0', 1)
    for i, row in df.iterrows():
        cname = row.cover_name.replace("'", "''")
        cid = row.cover_id
        q = """ UPDATE teams
                SET
                    covers='%s',
                    covers_id=%s
                WHERE team_id=%s
            """ % (cname, cid, row.team_id)
        cur.execute(q)
    conn.commit()
if __name__ == '__main__':
    conn2 = psycopg2.connect(database="seth", user="seth", password="abc123",
                        host="localhost", port="5432")
    df = pd.read_sql("""SELECT * FROM games_ss WHERE home_spread IS NOT NULL""", conn2)
    teams = pd.read_sql("""SELECT * FROM teams""", conn)
    for i, row in df.iterrows():
        
        try:
            home_id = teams[teams.ncaaid==row.home_team].team_id.values[0]
            away_id = teams[teams.ncaaid==row.away_team].team_id.values[0]
            if row.home_outcome == 'W':
                wid = home_id
                lid = away_id
            else:
                wid = away_id
                lid = home_id
            q = """ UPDATE reg_compact
                    SET home_spread=%s
                    WHERE dt='%s'
                    AND wteam=%s
                    AND lteam=%s
                """ % (row.home_spread, row.dt, wid, lid)
            cur.execute(q)
            print row.dt, wid, lid
        # break
        except:
            print row.home_team, row.away_team
            if row.home_team != 489 and row.away_team != 489:
                print 'error!'
                break

    conn.commit()