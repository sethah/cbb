import psycopg2
import pandas as pd
import numpy as np
from DataCollection.Crawlers.stats.ScrapeUtils import ScheduleScraper

CONN = psycopg2.connect(database="cbb", user="sethhendrickson",
                        password="abc123", host="localhost", port="5432")
CUR = CONN.cursor()
ALL_YEARS = range(2009, 2015)

def get_team_pages(year=None):
    if year is not None:
        years = range(year, year + 1)
    else:
        years = ALL_YEARS

    q = """SELECT ncaaid FROM teams WHERE ncaaid IS NOT NULL"""
    CUR.execute(q)
    results = CUR.fetchall()
    teams = [result[0] for result in results]
    urls = []
    for year in range(2009, 2015):
        year_code = ScheduleScraper.convert_ncaa_year_code(year)
        urls += ['http://stats.ncaa.org/team/index/%s?org_id=%s' % (year_code, team) for team in teams]

def sql_convert(values):
    """
    INPUT: NCAAScraper, 2D Numpy Array
    OUTPUT: 2D Numpy Array

    Convert floats to ints and nans to None
    """
    for i in xrange(len(values)):
        for j in xrange(len(values[0])):
            if type(values[i][j]) == float:
                if values[i][j].is_integer():
                    values[i][j] = int(values[i][j])
                elif np.isnan(values[i][j]):
                    values[i][j] = None
            elif values[i][j] == 'nan':
                values[i][j] = None
    return values

def get_existing_games():
    existing = pd.read_sql("SELECT * FROM games_test", CONN)
    existing['dt'] = existing['dt'].map(lambda x: str(x))

    return existing

def get_unplayed():
    unplayed = pd.read_sql("SELECT * FROM games_test WHERE game_id IS NULL", CONN)
    unplayed['dt'] = unplayed['dt'].map(lambda x: str(x))

    return unplayed

def insert_missing(scraped_df):
    existing1 = get_existing_games()
    existing2 =existing1.rename(columns={'hteam_id': 'ateam_id', 'ateam_id': 'hteam_id'})
    existing = pd.concat([existing1, existing2], axis=0)
    merged = scraped_df.merge(existing, how='left', on=['dt', 'hteam_id', 'ateam_id'])
    missing = merged[pd.isnull(merged.home_outcome_y)]
    to_insert = scraped_df.merge(missing[['dt', 'hteam_id', 'ateam_id']], on=['dt', 'hteam_id', 'ateam_id'])
    vals = sql_convert(to_insert.values)
    q =  """ INSERT INTO games_test
                (game_id, dt, hteam_id, ateam_id, opp_string, neutral, neutral_site,
                 home_outcome, numot, home_score, away_score)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
         """
    try:
        CUR.executemany(q, vals)
        CONN.commit()
    except:
        CONN.rollback()
        raise

def update_unplayed(scraped_df):
    unplayed = get_unplayed()
    to_update = scraped_df.merge(unplayed[['dt', 'hteam_id', 'ateam_id']], on=['dt', 'hteam_id', 'ateam_id'])
    q = """ UPDATE games_test
            SET home_score=%s,
                 away_score=%s,
                 neutral=%s,
                 neutral_site=%s,
                 home_outcome=%s,
                 numot=%s,
                 game_id=%s
            WHERE (dt=%s AND hteam_id=%s AND ateam_id=%s)
        """
    vals = to_update[['home_score', 'away_score', 'neutral', 'neutral_site', 'home_outcome',
                      'numot', 'game_id', 'dt', 'hteam_id', 'ateam_id']].values
    vals = sql_convert(vals)

    try:
        CUR.executemany(q, vals)
        CONN.commit()
    except:
        CONN.rollback()
        raise

def update_games_table():
    column_names = ['game_id', 'dt', 'hteam_id', 'ateam_id', 'opp_string', 'home_outcome',
                    'neutral_site', 'neutral', 'numot', 'home_score', 'away_score']
    scraped = pd.read_csv("output.csv", header=None, names=column_names)
    scraped = scraped.drop_duplicates('game_id')
    update_unplayed(scraped)
    insert_missing(scraped)

if __name__ == "__main__":
    update_games_table()
