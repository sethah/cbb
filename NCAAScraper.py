import requests
import httplib
import urllib2
import cookielib
from bs4 import BeautifulSoup
import pandas as pd
import re
import psycopg2
import numpy as np
from collections import defaultdict

CONN = psycopg2.connect(database="cbb", user="seth", password="abc123",
                        host="localhost", port="5432")
CUR = CONN.cursor()

class Page_Opener:

    def __init__(self):
        self.cookiejar = cookielib.LWPCookieJar()
        self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(self.cookiejar))
        self.agent = 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; NeosBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)'
        self.headers = {'User-Agent': self.agent}

    def open_and_soup(self, url, data=None):
        req = urllib2.Request(url, data=None, headers=self.headers)
        try:
            response = self.opener.open(req)
        except httplib.BadStatusLine as e:
            print e, e.line
        else:
            pass

        the_page = response.read()
        soup = BeautifulSoup(the_page)
        return soup

class NCAAScraper(object):

    def __init__(self):
        self.box_link_base = 'http://stats.ncaa.org/game/box_score/'
        self.pbp_link_base = 'http://stats.ncaa.org/game/play_by_play/'
        self.page_opener = Page_Opener()
        self.col_map = {'Min': 'Min', 'MP': 'Min', 'Tot Reb': 'Tot Reb',
                        'Pos': 'Pos', 'FGM': 'FGM', 'FGA': 'FGA', 
                        '3FG': '3FG', '3FGA': '3FGA','FT': 'FT', 
                        'FTA': 'FTA', 'PTS': 'PTS', 'Off Reb': 'Off Reb',
                        'ORebs': 'Off Reb', 'Def Reb': 'Def Reb', 
                        'DRebs': 'Def Reb', 'BLK': 'BLKS', 'BLKS': 'BLKS',
                        'ST': 'ST', 'STL': 'ST', 'Player': 'Player',
                        'AST': 'AST', 'TO': 'TO', 'Fouls': 'Fouls',
                        'Team': 'Team', 'game_id': 'game_id'}

    def pbp_link(self, game_id):
        return self.pbp_link_base + str(int(game_id))

    def pbp_rows(self, url):
        soup = self.page_opener.open_and_soup(url)

        rows = soup.findAll('tr')
        rows = [str(row) for row in rows if len(row.findAll('td',{'class' : 'smtext'})) > 0]
        return rows

    def game_id(self, url):
        return int(url.split('?')[0].split('/')[-1])

    def get_pbp_stats(self, url):
        soup = self.page_opener.open_and_soup(url)
        html_tables = soup.findAll('table', {'class': 'mytable'})
        htable = pd.read_html(str(html_tables[0]), header=0)[0]
        table = pd.read_html(str(html_tables[1]), skiprows=0, header=0, infer_types=False)[0]
        for i in xrange(2, len(html_tables)):
            table = pd.concat([table, pd.read_html(str(html_tables[i]), skiprows=0, header=0, infer_types=False)[0]])

        # table = table[table.Score != 'nan']

        return htable, table

    def time_to_dec(self, time_string, half):
        minutes, seconds = time_string.split(':')
        t = float(minutes) + float(seconds) / 60.

        if half == 0:
            return 20 - t
        if half == 1:
            return 40 - t
        else:
            return (40 + (half-1)*5) - t

    def string_to_stat(self, stat_string):
        stat_string = stat_string.upper()
        stat_list = ['MADE', 'MISSED', 'REBOUND', 'ASSIST', 'BLOCK',
                     'STEAL', 'TURNOVER', 'FOUL', 'TIMEOUT']
        shot_list = {'THREE POINT': 'TPM', 'FREE THROW': 'FTM',
                     'LAYUP': 'LUM', 'TWO POINT': 'JM',
                     'DUNK': 'DM', 'TIP': 'TIM'}
        rebound_list = {'OFFENSIVE': 'OREB', 'TEAM': 'TREB',
                        'DEFENSIVE': 'DREB'}
        for stat in stat_list:
            if stat in stat_string:
                break

        if stat == 'MADE' or stat == 'MISSED':
            for shot in shot_list:
                if shot in stat_string:
                    if stat == 'MISSED':
                        return shot_list[shot] + 'S'
                    else:
                        return shot_list[shot]
        elif stat == 'REBOUND':
            for rebound in rebound_list:
                if rebound in stat_string:
                    return rebound_list[rebound]
        else:
            return stat

    def format_pbp_stats(self, table):
        table.columns = ['Time', 'team1', 'Score', 'team2']
        d = defaultdict(list)
        hscores = []
        ascores = []
        times = []
        first_names = []
        last_names = []
        players = []
        plays = []
        team_ids = []
        half_subtractor = 20
        half = 0
        for i, row in table.iterrows():
            if row.Score == 'nan':
                half += 1
                continue
            ascore, hscore = row.Score.split('-')
            d['hscore'].append(hscore)
            d['ascore'].append(ascore)
            if row.team1 == 'nan':
                play_string = row.team2
                d['teamid'].append(1)
            else:
                play_string = row.team1
                d['teamid'].append(0)
            player = play_string.split()[0].strip()
            d['player'].append(player)
            if player == 'TEAM':
                last_name, first_name = '', player
            else:
                last_name, first_name = player.split(',')
            
            d['first_name'].append(first_name)
            d['last_name'].append(last_name)
            play = play_string.replace(player, '')
            play = self.string_to_stat(play)
            d['play'].append(play)
            t = self.time_to_dec(row.Time, half)

            d['time'].append(t)

        table = table[table.Score != 'nan']
        for col in d:
            table[col] = d[col]

        return table

    def get_box_stats(self, url):
        soup = self.page_opener.open_and_soup(url)
        tables = soup.findAll('table', {'class': 'mytable'})
        if len(tables) != 3:
            print 'Incorrect number of tables'
            return None

        htable = pd.read_html(str(tables[0]), header=0)[0]
        table1 = pd.read_html(str(tables[1]), skiprows=1, header=0, infer_types=False)[0]
        table2 = pd.read_html(str(tables[2]), skiprows=1, header=0, infer_types=False)[0]

        team1 = htable.iloc[0, 0]
        team2 = htable.iloc[1, 0]
        table1['Team'] = [team1] * table1.shape[0]
        table2['Team'] = [team2] * table2.shape[0]
        table1['game_id'] = [self.game_id(url)] * table1.shape[0]
        table2['game_id'] = [self.game_id(url)] * table2.shape[0]
        table1 = self.rename_box_table(table1)
        table2 = self.rename_box_table(table2)
        
        return htable, table1, table2

    def format_box_table(self, table):
        table['Min'] = table['Min'].map(lambda x: x.replace(':00', ''))
        format_cols = [col for col in table.columns \
                       if col not in {'Player', 'Pos', 'Team', 'game_id'}]
        
        # remove annoying characters from the cells
        chars_to_remove = ['*', '-', '/', u'\xc2']
        rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
        for col in format_cols:
            table[col] = table[col].map(lambda x: 
                                        re.sub(rx, '0', x)).astype(float)
        table['first_name'] = table.Player.map(lambda x: x.split(',')[-1].strip())
        table['last_name'] = table.Player.map(lambda x: x.split(',')[0].strip() if ',' in x else '')
        return table

    def sql_convert(self, values):
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

    def rename_box_table(self, table):
        d = {col: self.col_map[col] for col in table.columns}
        table = table.rename(columns=d)

        return table

    def scrape_box(self):
        q = """ SELECT game_id, box_link
                FROM games_ncaa
                WHERE game_id NOT IN
                    (SELECT DISTINCT(game_id) FROM ncaa_box)
                LIMIT 200
            """
        CUR.execute(q)
        results = CUR.fetchall()
        cols = ['game_id', 'Team', 'first_name', 'last_name',
                'Pos','Min','FGM', 'FGA', '3FG', '3FGA', 'FT', 'FTA',
                'PTS', 'Off Reb', 'Def Reb', 'Tot Reb', 'AST', 'TO', 'ST',
                'BLKS', 'Fouls']
        for idx, item in enumerate(results):
            try:
                print idx, item[1]
                htable, table1, table2 = scraper.get_box_stats(item[1])
                # assert False
                table1 = scraper.format_box_table(table1)
                table2 = scraper.format_box_table(table2)
                table = pd.concat([table1, table2])
                if idx == 0:
                    big_table = table
                else:
                    big_table = pd.concat([big_table, table])
            except urllib2.URLError:
                print 'Disconnected, storing progress'
            

        big_table = big_table[cols]
        q =  """ INSERT INTO ncaa_box (game_id, team, first_name, last_name,
                pos, min, fgm, fga, tpm, tpa, ftm, fta, pts, oreb, dreb, reb,
                ast, turnover, stl, blk, pf) VALUES (%s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        vals = self.sql_convert(big_table.values)
        CUR.executemany(q, vals)
        CONN.commit()

if __name__ == '__main__':
    scraper = NCAAScraper()
    htable, table = scraper.get_pbp_stats('http://stats.ncaa.org/game/play_by_play/3610784')
    table = scraper.format_pbp_stats(table)
    
    # vals = scraper.sql_convert(table.values)
    # table = scraper.scrape_box()
    # args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in tup)
    # cur.execute("INSERT INTO table VALUES " + args_str) 