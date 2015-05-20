import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, date, timedelta
import json
from collections import defaultdict

CONN = psycopg2.connect(host="127.0.0.1",
                    user="seth",
                    password="abc123",
                    database="cbb",
                    port="5432")
CUR = CONN.cursor()

season_dates = {
                '2011': {'start': date(2010, 11, 8), 'end': date(2011, 4, 4)},
                '2012': {'start': date(2011, 11, 9), 'end': date(2012, 4, 2)},
                '2013': {'start': date(2012, 11, 10), 'end': date(2013, 4, 8)},
                '2014': {'start': date(2013, 11, 8), 'end': date(2014, 4, 7)},
                '2015': {'start': date(2014, 11, 14), 'end': date(2015, 4, 6)}
                }

# def season_dates():
#     q = """ SELECT * FROM seasons"""
#     CUR.execute(q)
#     dates = []
#     for season in CUR.fetchall():
#         dates.append((season[0], season[1], date(season[0], 4, 15)))

#     return dates

def print_urls(dates):
    for season in dates:
        start_date = dates[season]['start']
        end_date = dates[season]['end']
        day_count = (end_date - start_date).days + 1
        for single_date in (start_date + timedelta(n) for n in xrange(day_count)):
            print 'http://stats.ncaa.org/team/schedule_list?academic_year={year}&division=1.0&sport_code=MBB&schedule_date={dt}'.format(year=season, dt=datetime.strftime(single_date, '%Y/%m/%d'))

def store_csv():
    with open('games.json') as f:
        data = json.load(f)
        d = defaultdict(list)
        data = data['data']
        categories = ['team1', 'team2', 'game_link', 'first_half1',
                      'first_half2', 'second_half1', 'second_half2',
                      'final', 'single_ot1', 'single_ot2', 'double_ot1',
                      'double_ot2']
        for item in data:
            d['dt'].append(item['_pageUrl'].split('=')[-1])
            for cat in categories:
                if cat in item:
                    if cat == 'final':
                        d['final1'].append(int(item[cat][0]))
                        d['final2'].append(int(item[cat][1]))
                    elif cat not in {'team1', 'team2', 'game_link'}:
                        d[cat].append(int(item[cat][0]))
                    else:
                        d[cat].append(item[cat][0])
                else:
                    if cat == 'final':
                        d['final1'].append(None)
                        d['final2'].append(None)
                    else:
                        d[cat].append(np.nan)

    df = pd.DataFrame(d)
    df['dt'] = df.dt.map(lambda x: pd.to_datetime(x))
    df['game_id'] = df.game_link.fillna('')
    df['game_id'] = df.game_id.map(lambda x: \
                                    x.split('?')[0].split('/')[-1] \
                                    if x != '' else x)
    df.game_id[df.game_id == ''] = np.nan
    df.game_id = df.game_id.astype(float)
    df['box_link'] = df.game_id.map(lambda x: \
                                    'http://stats.ncaa.org/game/box_score/' + str(int(x))
                                    if ~np.isnan(x) else x)
    df['pbp_link'] = df.game_id.map(lambda x: \
                                    'http://stats.ncaa.org/game/play_by_play/' + str(int(x))
                                    if ~np.isnan(x) else x)
    cols = ['dt', 'team1', 'team2', 'first_half1', 'first_half2', \
            'second_half1', 'second_half2', 'single_ot1', 'single_ot2',
            'double_ot1', 'double_ot2', 'final1', 'final2',
            'game_id', 'box_link', 'pbp_link']
    df = df[cols]

    df.to_csv('games_corrected.csv', index=False)

if __name__ == '__main__':
    # dates = season_dates()
    # print_urls(season_dates)
    store_csv()
    



    