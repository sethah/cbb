import psycopg2
import pandas as pd
import numpy as np

from DataCollection.NCAAStatsUtil import NCAAStatsUtil as ncaa_util
from DataCollection.DB import DB


CONN = DB.conn
CUR = CONN.cursor()
ALL_YEARS = range(2009, 2015)
TABLE_NAMES_MAP = {'box': 'box_test', 'pbp': 'raw_pbp'}

def insert_box_stats(box_table):
    """
    INPUT: NCAAScraper
    OUTPUT: None

    Scrape, format, and store box data
    """

    q =  """ INSERT INTO box_test (game_id, team, first_name, last_name,
            pos, min, fgm, fga, tpm, tpa, ftm, fta, pts, oreb, dreb, reb,
            ast, turnover, stl, blk, pf) VALUES (%s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    vals = sql_convert(box_table.values)
    try:
        CUR.executemany(q, vals)
        CONN.commit()
    except:
        print vals
        CONN.rollback()

def get_games_to_scrape(year=None, from_table='box', num_games=500):
    """Get a list of games that haven't been scraped"""
    if from_table == 'box':
        table = TABLE_NAMES_MAP.get(from_table)
    else:
        table = TABLE_NAMES_MAP.get(from_table)
    assert table, "From table must be in %s" % TABLE_NAMES_MAP.keys()

    if year is not None:
        year_filter = "AND EXTRACT(YEAR FROM dt)={year}".format(year=year)
    else:
        year_filter = ""

    q = """ SELECT game_id
            FROM games_test
            WHERE game_id NOT IN
                (SELECT DISTINCT(game_id) FROM box_test)
            AND game_id IS NOT NULL
            AND game_id NOT IN (SELECT game_id FROM url_errors)
            {year_filter}
            ORDER BY DT DESC
            LIMIT {num_games}
        """.format(table=table, year_filter=year_filter, num_games=num_games,
                   check_link=from_table)
    print q
    CUR.execute(q)
    results = CUR.fetchall()
    results = [ncaa_util.stats_link(x[0], from_table) for x in results]
    return results

def get_team_pages(year=None):
    """Generate a list of team page urls for given year"""
    if year is not None:
        years = range(year, year + 1)
    else:
        years = ALL_YEARS

    q = """SELECT ncaaid FROM teams WHERE ncaaid IS NOT NULL"""
    CUR.execute(q)
    results = CUR.fetchall()
    teams = [result[0] for result in results]
    urls = []
    for year in years:
        year_code = ncaa_util.convert_ncaa_year_code(year)
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
            elif type(values[i][j]) == np.float64:
                if np.isnan(values[i][j]):
                    values[i][j] = None
    return values

def get_existing_games():
    """Return all existing games from the games database"""
    existing = pd.read_sql("SELECT * FROM games_test", CONN)
    existing['dt'] = existing['dt'].map(lambda x: str(x))

    return existing

def get_unplayed():
    """Return a list of games that don't have game ids from games db"""
    unplayed = pd.read_sql("SELECT * FROM games_test WHERE game_id IS NULL", CONN)
    unplayed['dt'] = unplayed['dt'].map(lambda x: str(x))

    return unplayed

def insert_missing(scraped_df):
    """From scraped games, insert only ones that are not in the database already"""
    existing1 = get_existing_games()
    existing2 = existing1.rename(columns={'hteam_id': 'ateam_id', 'ateam_id': 'hteam_id'})
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
    """Update game entries in the database that were previously unplayed"""
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

def insert_pbp_data(values):
    values = sql_convert(values)
    q =  """ INSERT INTO pbp
                (game_id, pbp_id, team, teamid, time, first_name, last_name,
                 play, hscore, ascore, possession, poss_time_full,
                 poss_time, home_fouls, away_fouls, second_chance,
                 timeout_pts, turnover_pts, and_one, blocked, stolen,
                 assisted, assist_play, recipient, charge)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
         """

    try:
        CUR.executemany(q, values)
        CONN.commit()
    except:
        CONN.rollback()
        raise

def update_games_table():
    """Routine to update the games table from a list of scraped games"""
    column_names = ['game_id', 'dt', 'hteam_id', 'ateam_id', 'opp_string', 'home_outcome',
                    'neutral_site', 'neutral', 'numot', 'home_score', 'away_score']
    scraped = pd.read_csv("output.csv", header=None, names=column_names)
    scraped = scraped.drop_duplicates('game_id')
    update_unplayed(scraped)
    insert_missing(scraped)

