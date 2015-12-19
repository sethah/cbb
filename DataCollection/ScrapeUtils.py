from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import re
from collections import defaultdict

from DB import DB

from DataCollection.NCAAStatsUtil import NCAAStatsUtil as ncaa_util
from DataCollection.DBScrapeUtils import sql_convert

class ScheduleScraper(object):

    def __init__(self):
        pass

    @classmethod
    def get_team_schedule(cls, soup, url):
        """
        INPUT: BeautifulSoup, string
        OUTPUT: 2D-Array

        Get a 2D array representation of the team's scheduled games including various
        information about each game.
        """
        team_id = ncaa_util.get_team_id(url)
        tables = soup.findAll('table', {'class': 'mytable'})
        if len(tables) > 0:
            schedule_table = tables[0]
        else:
            return []
        table_rows = schedule_table.findAll('tr')
        games = []
        for idx, row in enumerate(table_rows):
            # skip the title row and header row
            if idx < 2:
                continue

            game_info = cls._process_schedule_row(row, team_id)
            if game_info is not None:
                games.append(game_info)

        return games

    @classmethod
    def _process_schedule_row(cls, row, team_id):
        """Extract useful information about a game from its row representation"""
        tds = row.findAll('td')
        if len(tds) != 3:
            return None
        date_string = tds[0].get_text()
        game_date = datetime.strptime(date_string, '%m/%d/%Y').date()
        opp_link = tds[1].find('a')
        opp_text = tds[1].get_text()
        if opp_link is not None:
            opp_id = ncaa_util.get_team_id(opp_link['href'])
        else:
            opp_id = None
        opp, neutral_site, loc = ncaa_util.parse_opp_string(opp_text)
        if loc == 'A':
            hteam_id = opp_id
            ateam_id = team_id
        else:
            hteam_id = team_id
            ateam_id = opp_id
        neutral = True if neutral_site else False
        outcome_string = tds[2].get_text()
        game_link = tds[2].find('a')
        if game_link is not None:
            game_url = game_link['href']
            game_id = ncaa_util.parse_link(game_url)
        else:
            game_id = None

        outcome, score, opp_score, num_ot = ncaa_util.parse_outcome(outcome_string)
        home_score, away_score, home_outcome = cls.process_score(score, opp_score, loc)

        return [game_id, game_date, hteam_id, ateam_id, opp, neutral,
                neutral_site, home_outcome, num_ot, home_score, away_score]

    @staticmethod
    def process_score(score, opp_score, loc):
        """
        Derive home team and away team from team, opponent, and team location
        Note: neutral games will default to the current team being home team, though
              this should not matter since the neutral site information is also
              captured.
        """
        if loc == 'A':
            home_score = opp_score
            away_score = score
        else:
            home_score = score
            away_score = opp_score
        home_outcome = home_score > away_score
        return home_score, away_score, home_outcome

class BoxScraper(object):

    @classmethod
    def extract_box_stats(cls, soup, url):
        """
        INPUT: BeautifulSoup, STRING
        OUTPUT: DATAFRAME, DATAFRAME

        Extract box stats from html and convert to dataframe

        url is a string linking to the box stats page
        """
        tables = soup.findAll('table', {'class': 'mytable'})
        if len(tables) != 3:
            print 'Incorrect number of tables'
            return None

        htable = pd.read_html(str(tables[0]), header=0)[0]
        table1 = pd.read_html(str(tables[1]), skiprows=1, header=0, infer_types=False)[0]
        table2 = pd.read_html(str(tables[2]), skiprows=1, header=0, infer_types=False)[0]

        team1 = htable.iloc[0, 0]
        team2 = htable.iloc[1, 0]
        table1['Team'] = team1
        table2['Team'] = team2

        # assign a game_id column with all values equal to game_id
        table1['game_id'] = ncaa_util.parse_stats_link(url)
        table2['game_id'] = ncaa_util.parse_stats_link(url)

        # older box stat page versions use different column names so
        # we must map them all to common column names (e.g. MIN vs. Min)
        table1 = cls.rename_box_table(table1)
        table2 = cls.rename_box_table(table2)
        table1 = cls.format_box_table(table1)
        table2 = cls.format_box_table(table2)

        box_table = cls._combine_box_tables(table1, table2)

        return htable, box_table[ncaa_util.box_columns]

    @classmethod
    def _combine_box_tables(cls, table1, table2):
        """Combine the two teams' box stats into one dataframe"""
        assert table1.shape[1] == table2.shape[1], \
            "table1 ncols = %s did not match table2 ncols = %s" % (table1.shape[1], table2.shape[1])
        return pd.concat([table1, table2])

    @classmethod
    def format_box_table(cls, table):
        """
        INPUT: DATAFRAME
        OUTPUT: DATAFRAME

        Format the box table to prepare for storage by removing unwanted characters, etc...

        table is a dataframe containing raw box stats
        """
        table.dropna(axis=0, subset=['Player'], inplace=True)

        # minutes column is in form MM:00
        table['Min'] = table['Min'].map(lambda x: x.replace(':00', '') if ':00' in ncaa_util.clean_string(x) else '0')

        do_not_format = {'Player', 'Pos', 'Team', 'game_id'}
        format_cols = filter(lambda col: col not in do_not_format, table.columns)

        # remove annoying characters from the cells
        chars_to_remove = ['*', '-', '/', u'\xc2']
        rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
        for col in format_cols:
            # need to remove garbage characters if column type is object
            if table[col].dtype == np.object:
                table[col] = table[col].map(lambda x: re.sub(rx, '', ncaa_util.clean_string(x)))
                # we are trying to handle case where entire column is empty
                table[col] = table[col].map(lambda x: np.nan if x == '' else x)
                # converts empty strings to nans, but does nothing when entire column is empty strings
                table[col] = table[col].convert_objects(convert_numeric=True)

        table['first_name'] = table.Player.map(lambda x: ncaa_util.parse_name(x)[0])
        table['last_name'] = table.Player.map(lambda x: ncaa_util.parse_name(x)[1])

        return table

    @classmethod
    def rename_box_table(cls, table):
        """Map all columns to the same name"""

        d = {col: ncaa_util.col_map[col] for col in table.columns}
        table = table.rename(columns=d)

        return table


class PBPScraper(object):

    @classmethod
    def extract_pbp_stats(cls, soup):
        """
        INPUT: NCAAScraper, STRING
        OUTPUT: DATAFRAME, DATAFRAME

        Extract the pbp data and the game summary table from the pbp
        data page for a game.

        url is a string which links to the pbp page
        """
        html_tables = soup.findAll('table', {'class': 'mytable'})
        htable = pd.read_html(str(html_tables[0]), header=0)[0]
        table = pd.read_html(str(html_tables[1]), skiprows=0, header=0, infer_types=False)[0]
        for i in xrange(2, len(html_tables)):
            table = pd.concat([table, pd.read_html(str(html_tables[i]), skiprows=0, header=0, infer_types=False)[0]])

        table['game_id'] = ncaa_util.parse_stats_link(url)
        table = cls.format_pbp_stats(table, htable)

        return htable, table

    @classmethod
    def format_pbp_stats(cls, table, htable):
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
            play, first_name, last_name = ncaa_util.split_play(play_string)
            play = ncaa_util.string_to_stat(play)

            t = ncaa_util.time_to_dec(row.Time, half)

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

class DivisionOneScraper(object):

    @classmethod
    def get_urls(cls, years):
        base = 'http://stats.ncaa.org/team/inst_team_list?'
        urls = []
        for year in years:
            urls.append('{base}academic_year={year}&division=1&sport_code=MBB'.format(base=base,
                                                                             year=year))
        return urls

    @classmethod
    def extract_teams(cls, soup):
        atags = soup.findAll('a')
        atags = filter(lambda a: 'team/index' in a['href'], atags)
        ncaaids = [ncaa_util.get_team_id(a['href']) for a in atags]
        ncaa_names = [a.get_text().strip() for a in atags]

        assert len(ncaaids) == len(ncaa_names)

        return ncaaids, ncaa_names

    @classmethod
    def insert_data(cls, df):
        existing_data = pd.read_sql("SELECT * FROM division_one", DB.conn)
        merged = df.merge(existing_data, how='left', left_on=["teamid", "year"], right_on=["ncaaid", "year"])
        missing = merged[pd.isnull(merged.ncaaid)]
        vals = sql_convert(missing[['teamid', 'year']].values)
        cur = DB.conn.cursor()
        q =  """ INSERT INTO division_one
                    (ncaaid, year)
                 VALUES (%s, %s)
             """
        try:
            cur.executemany(q, vals)
            DB.conn.commit()
        except:
            DB.conn.rollback()
            raise

class KenpomScraper(object):

    @classmethod
    def get_urls(cls, years):
        base = "http://kenpom.com/index.php?y="
        urls = []
        for year in years:
            urls.append('{base}{year}'.format(base=base, year=year))
        return urls

    @classmethod
    def get_year(cls, url):
        pattern = "y=[0-9]+"
        substring = re.search(pattern, url).group()
        if substring is not None:
            return int(substring.split("y=")[-1])
        else:
            raise ValueError, "couldn't find the year"


    @classmethod
    def extract_teams(cls, soup, year):
        table = soup.find('table', {'id': 'ratings-table'})
        # tbodys = soup.findAll()
        def filter_tr(tr):
            tds = tr.findAll('td')
            if len(tds) > 0:
                if str(tds[0].get_text()).isdigit():
                    return True
            return False
        trs = filter(filter_tr, soup.findAll('tr'))
        theader = '<table>'
        tbody = "".join([str(tr) for tr in trs])
        ttail = '</table>'
        table = theader + tbody + ttail
        columns = ['rank', 'team', 'conf', 'wl', 'pyth', 'adjo', 'adjo_rank',
                   'adjd', 'adjd_rank', 'adjt', 'adjt_rank', 'luck', 'luck_rank',
                   'sos_pyth', 'sos_pyth_rank', 'sos_opp_o', 'sos_opp_o_rank',
                   'sos_opp_d', 'sos_opp_d_rank', 'ncsos', 'ncsos_rank']
        df = pd.read_html(table, infer_types=False)[0]
        def clean_team(s):
            s = s.replace(";", "")
            pattern = '( [0-9]+)'
            splits = re.split(pattern, s)
            if len(splits) == 3:
                return splits[0].strip()
            else:
                return s
        df.columns = columns
        df['team'] = df.team.map(lambda team: clean_team(team))
        df['wins'] = df.wl.map(lambda x: int(x.split('-')[0]))
        df['losses'] = df.wl.map(lambda x: int(x.split('-')[1]))
        df['year'] = year
        return df

    @classmethod
    def insert_data(cls, df):
        cur = DB.conn.cursor()
        years = np.unique(df.year.values)
        # delete the data for every year we are trying to update
        for year in years:
            q = "DELETE FROM kenpom_ranks WHERE year=%s" % int(year)
            cur.execute(q)

        cols_to_insert = ['year', 'rank', 'wins', 'losses', 'team', 'conf',
                          'pyth', 'adjo', 'adjd', 'adjt', 'luck', 'sos_pyth',
                          'sos_opp_o', 'sos_opp_d', 'ncsos']
        vals = sql_convert(df[cols_to_insert].values)
        column_insert = '(' + ", ".join(cols_to_insert) + ')'
        vals_insert = '(' + ", ".join(["%s"] * len(cols_to_insert)) + ')'
        q = """ INSERT INTO kenpom_ranks %s VALUES %s""" % (column_insert, vals_insert)
        try:
            cur.executemany(q, vals)
            DB.conn.commit()
        except Exception, e:
            print e
            DB.conn.rollback()
            raise


if __name__ == "__main__":
    pass
