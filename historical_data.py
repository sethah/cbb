import pandas as pd
import numpy as np
import psycopg2
import json
import re

NCAA_YEARS = {'2010': 10260,
              '2011': 10440,
              '2012': 10740,
              '2013': 11220,
              '2014': 11540,
              '2015': 12020}

CONN = psycopg2.connect(database="cbb", user="seth", password="abc123",
                        host="localhost", port="5432")
CUR = CONN.cursor()

def print_urls():
    base_url = 'http://stats.ncaa.org/team/index/'
    teams_q = """ SELECT ncaaid, ncaa
                  FROM teams
                  WHERE ncaaid IS NOT NULL
              """
    CUR.execute(teams_q)
    for team in CUR.fetchall():
        for year in NCAA_YEARS:
            print '%s%s?org_id=%s' % (base_url, NCAA_YEARS[year], team[0])

def clean_games():
    df = pd.read_csv('schedule_games.csv')
    df['team1_ncaaid'] = df._pageUrl.map(lambda x: int(x.split('=')[-1])).astype(float)
    df['team2_ncaaid'] = df.opp_link.map(lambda x: int(x.split('=')[-1]) \
                                        if len(str(x)) > 30 else x).astype(float)
    df['game_key'] = df.apply(lambda row: ','.join(sorted([str(row.team1_ncaaid), str(row.team2_ncaaid)])), axis=1)
    df = df.drop_duplicates(['game_key', 'date'])

    return df

def format_games(df):
    cols = ['team1_ncaaid', 'team2_ncaaid', 'date',
            'game_link', 'outcome', 'opp']
    df = df[cols]

    print df.shape
    # get home and away teams
    df['ateam_id'] = df.apply(lambda row: row.team1_ncaaid if row.opp[0] == '@' else row.team2_ncaaid, axis=1)
    df['hteam_id'] = df.apply(lambda row: row.team2_ncaaid if row.ateam_id == row.team1_ncaaid else row.team1_ncaaid, axis=1)

    df['opp_string'] = df.opp.map(lambda x: x.split('@')[-1].strip() if x[0] == '@' \
                                  else x.split('@')[0].strip())

    # get game_id
    df['game_id'] = df.game_link.map(lambda x: \
                                    x.split('?')[0].split('/')[-1] \
                                    if len(str(x)) > 30 else x)

    # get pbp and box link
    df.game_id = df.game_id.astype(float)
    df['box_link'] = df.game_id.map(lambda x: \
                                    'http://stats.ncaa.org/game/box_score/' + str(int(x))
                                    if ~np.isnan(x) else x)
    df['pbp_link'] = df.game_id.map(lambda x: \
                                    'http://stats.ncaa.org/game/play_by_play/' + str(int(x))
                                    if ~np.isnan(x) else x)

    # get neutral site
    df['neutral'] = df.opp.map(lambda x: ('@' in x) and (x[0] != '@'))

    # scores and outcome
    df['final1'] = df.outcome.map(lambda x: re.sub("[^0-9]", "", x.split('-')[0]).strip())#.astype(int)
    df['final2'] = df.outcome.map(lambda x: re.sub("[^0-9]", "", x.split('-')[1]).strip())#.astype(int)
    df['home_score'] = df.apply(lambda row: row.final1 if row.hteam_id == row.team1_ncaaid else row.final2, axis=1)
    df['away_score'] = df.apply(lambda row: row.final1 if row.ateam_id == row.team1_ncaaid else row.final2, axis=1)
    df['home_outcome'] = df.home_score > df.away_score
    df['numot'] = df.outcome.map(lambda x: int(x.split('OT)')[0].split('(')[-1]) \
                                 if 'OT)' in x else 0)


    # convert float cols to int strings
    df.hteam_id = df.hteam_id.map(lambda x: str(int(x)) if ~np.isnan(x) else x)
    df.ateam_id = df.ateam_id.map(lambda x: str(int(x)) if ~np.isnan(x) else x)
    df.game_id = df.game_id.map(lambda x: str(int(x)) if ~np.isnan(x) else x)

    cols = ['date', 'hteam_id', 'ateam_id', 'home_score', 'away_score',
            'neutral', 'home_outcome', 'numot', 'box_link', 'pbp_link',
            'game_id', 'opp_string']
    return df[cols]

if __name__ == '__main__':
    # # print_urls()
    df = clean_games()
    df = format_games(df)
    teams = pd.read_sql("""SELECT * FROM teams""", CONN)