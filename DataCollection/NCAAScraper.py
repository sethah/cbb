import httplib
import urllib2
import cookielib
from bs4 import BeautifulSoup
import pandas as pd
import re
import psycopg2
import numpy as np
from collections import defaultdict

class Page_Opener(object):

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
        soup = BeautifulSoup(the_page, "lxml")
        return soup

class NCAAScraper(object):
    box_link_base = 'http://stats.ncaa.org/game/box_score/'
    pbp_link_base = 'http://stats.ncaa.org/game/play_by_play/'
    box_columns = ['game_id', 'Team', 'first_name', 'last_name',
                   'Pos','Min','FGM', 'FGA', '3FG', '3FGA', 'FT', 'FTA',
                   'PTS', 'Off Reb', 'Def Reb', 'Tot Reb', 'AST', 'TO', 'ST',
                   'BLKS', 'Fouls']
    table_names = {'box': 'ncaa_box', 'pbp': 'raw_pbp'}

    def __init__(self, conn):
        """
        INPUT: NCAAScraper
        OUTPUT: None

        Initialize an ncaa stats scraper object

        This object contains methods to pull box and play-by-play data
        from stats.ncaa.org, process that data, and insert it into a 
        PostgreSQL database.
        """
        self.conn = conn
        self.cur = self.conn.cursor()
        self.page_opener = Page_Opener()
        self.col_map = {'Min': 'Min', 'MP': 'Min', 'Tot Reb': 'Tot Reb',
                        'Pos': 'Pos', 'FGM': 'FGM', 'FGA': 'FGA', 
                        '3FG': '3FG', '3FGA': '3FGA','FT': 'FT', 
                        'FTA': 'FTA', 'PTS': 'PTS', 'Off Reb': 'Off Reb',
                        'ORebs': 'Off Reb', 'Def Reb': 'Def Reb', 
                        'DRebs': 'Def Reb', 'BLK': 'BLKS', 'BLKS': 'BLKS',
                        'ST': 'ST', 'STL': 'ST', 'Player': 'Player',
                        'AST': 'AST', 'TO': 'TO', 'Fouls': 'Fouls',
                        'Team': 'Team', 'game_id': 'game_id', 'Time': 'Time'}

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
        """
        INPUT: NCAAScraper, STRING
        OUTPUT: DATAFRAME, DATAFRAME

        Extract the pbp data and the game summary table from the pbp 
        data page for a game.

        url is a string which links to the pbp page
        """
        soup = self.page_opener.open_and_soup(url)
        html_tables = soup.findAll('table', {'class': 'mytable'})
        htable = pd.read_html(str(html_tables[0]), header=0)[0]
        table = pd.read_html(str(html_tables[1]), skiprows=0, header=0, infer_types=False)[0]
        for i in xrange(2, len(html_tables)):
            table = pd.concat([table, pd.read_html(str(html_tables[i]), skiprows=0, header=0, infer_types=False)[0]])

        table['game_id'] = [self.game_id(url)] * table.shape[0]

        return htable, table

    def time_to_dec(self, time_string, half):
        """
        INPUT: NCAAScraper, STRING, INT
        OUTPUT: FLOAT

        Convert a time string 'MM:SS' remaining in a half to a 
        float representing absolute time elapsed

        time_string is a string in the form 'MM:SS'
        half is an integer representing which half the game is in (>1 is OT)
        """
        # some rows may not have valid time strings
        if ':' not in time_string:
            return -1

        minutes, seconds = time_string.split(':')
        t = float(minutes) + float(seconds) / 60.

        if half == 0:
            return 20 - t
        if half == 1:
            return 40 - t
        else:
            return (40 + (half-1)*5) - t

    def string_to_stat(self, stat_string):
        """
        INPUT: NCAAScraper, STRING
        OUTPUT: STRING

        Convert a string which describes an action into a unique encoded 
        string representing that action

        stat_string is a string representing the action (e.g. 'missed dunk')
        """
        stat_string = stat_string.upper()
        stat_list = ['MADE', 'MISSED', 'REBOUND', 'ASSIST', 'BLOCK',
                     'STEAL', 'TURNOVER', 'FOUL', 'TIMEOUT', 'ENTERS',
                     'LEAVES']
        shot_list = {'THREE POINT': 'TPM', 'FREE THROW': 'FTM',
                     'LAYUP': 'LUM', 'TWO POINT': 'JM',
                     'DUNK': 'DM', 'TIP': 'TIM'}
        rebound_list = {'OFFENSIVE': 'OREB', 'TEAM': 'TREB',
                        'DEFENSIVE': 'DREB', 'DEADBALL': 'DEADREB'}
        stat = None
        for st in stat_list:
            if st in stat_string:
                stat = st
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

    def split_play(self, play_string):
        """
        INPUT: NCAAScraper, STRING
        OUTPUT: STRING, STRING, STRING

        Split the raw full play string into a play, first name, and last name

        play_string describes the play (e.g. DOE, JOHN made jumper)

        WARNING: This will not work when the player's name is not in all caps.
        Some of the older pbp pages do not use this pattern and so the player's
        name will not be extracted properly
        """
        pattern = r"^[A-Z,\s'\.-]+\b"
        rgx = re.match(pattern, play_string)
        if rgx is None:
            player = ''
        else:
            player = rgx.group(0).strip()
        play = play_string.replace(player, '').strip()
        if player == 'TEAM' or player == 'TM':
            last_name, first_name = '', 'TEAM'
        elif ',' not in player:
            print 'Bad player string', player
            last_name, first_name = ('', '')
        else:
            splits = player.split(',')
            last_name, first_name = splits[0], splits[1]

        return play, first_name, last_name

    def format_pbp_stats(self, table, htable):
        """
        INPUT: NCAAScraper, DATAFRAME, DATAFRAME
        OUTPUT: DATAFRAME

        Convert the raw tables into tabular data for storage.

        table is a dataframe containing raw pbp data
        htable is a dataframe containing game summary info
        """
        table.columns = ['Time', 'team1', 'Score', 'team2', 'game_id']
        d = defaultdict(list)
        half = 0
        for i, row in table.iterrows():
            if row.Score == 'nan':
                half += 1
                continue
            if row.team1 == 'nan':
                play_string = row.team2
                d['teamid'].append(1)
            else:
                play_string = row.team1
                d['teamid'].append(0)
            play, first_name, last_name = self.split_play(play_string)
            play = self.string_to_stat(play)

            t = self.time_to_dec(row.Time, half)

            ascore, hscore = row.Score.split('-')
            d['hscore'].append(hscore)
            d['ascore'].append(ascore)
            
            d['first_name'].append(first_name)
            d['last_name'].append(last_name)
            
            d['play'].append(play)

            d['time'].append(t)

        # if the score is nan then it is a end of half row
        cond1 = table.Score != 'nan'
        
        table = table[cond1]
        for col in d:
            table[col] = d[col]

        cond2 = table.time > 0
        table = table[cond2]
        team1 = htable.iloc[0, 0]
        team2 = htable.iloc[1, 0]
        table['team'] = table.teamid.map(lambda x: team1 if x==0 else team2)

        keep_cols = ['game_id', 'time', 'teamid', 'team', 'first_name',
                     'last_name', 'play', 'hscore', 'ascore']
        return table[keep_cols]

    def get_box_stats(self, url):
        """
        INPUT: NCAAScraper, STRING
        OUTPUT: DATAFRAME, DATAFRAME, DATAFRAME

        Extract html from box stats page and convert to dataframes

        url is a string linking to the box stats page
        """
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

        # older box stat page versions use different column names so
        # we must map them all to common column names (e.g. MIN vs. Min)
        table1 = self.rename_box_table(table1)
        table2 = self.rename_box_table(table2)
        table1 = self.format_box_table(table1)
        table2 = self.format_box_table(table2)
        
        return htable, table1, table2

    def format_box_table(self, table):
        """
        INPUT: NCAAScraper, DATAFRAME
        OUTPUT: DATAFRAME

        Format the box table to prepare for storage

        table is a dataframe containing raw box stats
        """
        # minutes column is in form MM:00
        table['Min'] = table['Min'].map(lambda x: x.replace(':00', ''))
        format_cols = [col for col in table.columns \
                       if col not in {'Player', 'Pos', 'Team', 'game_id'}]
        
        # remove annoying characters from the cells
        chars_to_remove = ['*', '-', '/', u'\xc2']
        rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
        for col in format_cols:
            # need to remove garbage characters if column type is object
            if table[col].dtype == np.object:
                table[col] = table[col].map(lambda x: re.sub(rx, '0', x)).astype(float)

        table['first_name'] = table.Player.map(lambda x: x.split(',')[-1].strip())
        table['last_name'] = table.Player.map(lambda x: x.split(',')[0].strip() if ',' in x else '')
        return table

    def sql_convert(self, values):
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

    def rename_box_table(self, table):
        """Map all columns to the same name"""

        d = {col: self.col_map[col] for col in table.columns}
        table = table.rename(columns=d)

        return table

    def get_games_to_scrape(self, year, from_table='box', num_games=500):
        if from_table == 'box':
            table = NCAAScraper.table_names.get(from_table)
        else:
            table = NCAAScraper.table_names.get(from_table)
        assert table, "From table must be in %s" % NCAAScraper.table_names.keys()

        q = """ SELECT game_id, box_link, pbp_link, dt
                FROM games_ncaa
                WHERE game_id NOT IN
                    (SELECT DISTINCT(game_id) FROM {table})
                AND {check_link}_link IS NOT NULL
                AND game_id NOT IN (SELECT game_id FROM url_errors)
                AND EXTRACT(YEAR FROM dt)={year}
                ORDER BY DT DESC
                LIMIT {num_games}
            """.format(table=table, year=year, num_games=num_games,
                       check_link=from_table)
        self.cur.execute(q)
        results = self.cur.fetchall()
        return results

    def scrape_box(self, game_list):
        """
        INPUT: NCAAScraper
        OUTPUT: None

        Scrape, format, and store box data
        """
        assert len(game_list[0]) == 4, "game list must be a four tuple"

        cols = NCAAScraper.box_columns
        for idx, (gameid, box_link, pbp_link, dt) in enumerate(game_list):
            try:
                print idx, box_link
                htable, table1, table2 = scraper.get_box_stats(box_link)
                table = pd.concat([table1, table2])
                table = table[cols]
                q =  """ INSERT INTO ncaa_box (game_id, team, first_name, last_name,
                        pos, min, fgm, fga, tpm, tpa, ftm, fta, pts, oreb, dreb, reb,
                        ast, turnover, stl, blk, pf) VALUES (%s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                vals = self.sql_convert(table.values)
                self.cur.executemany(q, vals)
            except urllib2.URLError, HTTPError:
                # store the pages that don't load
                q = """ INSERT INTO url_errors
                        (game_id)
                        VALUES (%s)
                    """ % gameid
                self.cur.execute(q)
                print 'URL or HTTP Error'
                continue
            except Exception, e:
                print str(e)
                self.conn.commit()
                raise

        self.conn.commit()

    def scrape_pbp(self, game_list):
        """
        INPUT: NCAAScraper
        OUTPUT: None

        Scrape, format, and store pbp data
        """
        assert len(game_list[0]) == 4, "game list must be a four tuple"
        # some pages won't load so they are stored in 'url_errors' table 
        # so we don't try them again
        for idx, (gameid, box_link, pbp_link, dt) in enumerate(game_list):
            if pbp_link == '':
                continue
            try:
                print idx, pbp_link
                htable, table = self.get_pbp_stats(pbp_link)
                table = scraper.format_pbp_stats(table, htable)
                q =  """ INSERT INTO raw_pbp 
                            (game_id, time, teamid, team, first_name, 
                             last_name, play, hscore, ascore) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                     """
                vals = self.sql_convert(table.values)
                self.cur.executemany(q, vals)
            except urllib2.URLError, HTTPError:
                # store the pages that don't load
                # TODO: this needs to be handled more correctly
                q = """ INSERT INTO url_errors
                        (game_id)
                        VALUES (%s)
                    """ % gameid
                self.cur.execute(q)
                print 'URL or HTTP Error'
                continue
            except Exception, e:
                print str(e)
                self.conn.commit()

        self.conn.commit()

if __name__ == '__main__':
    conn = psycopg2.connect(database="sethhendrickson", user="sethhendrickson",
                            password="abc123", host="localhost", port="5432")
    scraper = NCAAScraper(conn)
    link = 'http://stats.ncaa.org/game/box_score/3545511'
    games = scraper.get_games_to_scrape(2015, 'box', 1000)
    scraper.scrape_box(games)
