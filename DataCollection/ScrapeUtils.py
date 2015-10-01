from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import re
import string

from DataCollection.NCAAStatsUtil import NCAAStatsUtil as ncaa_util

class ScheduleScraper(object):

    def __init__(self):
        pass

    @classmethod
    def get_team_schedule(cls, soup, url):
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
        INPUT: NCAAScraper, STRING
        OUTPUT: DATAFRAME, DATAFRAME, DATAFRAME

        Extract html from box stats page and convert to dataframes

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
        assert table1.shape[1] == table2.shape[1], \
            "table1 ncols = %s did not match table2 ncols = %s" % (table1.shape[1], table2.shape[1])
        return pd.concat([table1, table2])

    @classmethod
    def format_box_table(cls, table):
        """
        INPUT: NCAAScraper, DATAFRAME
        OUTPUT: DATAFRAME

        Format the box table to prepare for storage

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
                table[table[col] == ''] = np.nan
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


if __name__ == "__main__":
    urls = ["http://stats.ncaa.org/game/box_score/1460512"]
    url = urls[0]
    import requests
    from bs4 import BeautifulSoup
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    print response.status_code
    htable, box_table = BoxScraper.extract_box_stats(soup, url)
