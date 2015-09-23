import pandas as pd
import re
from datetime import datetime, date, timedelta

class ScheduleScraper(object):

    def __init__(self):
        pass

    @classmethod
    def get_team_schedule(cls, soup):
        schedule_table = soup.findAll('table', {'class': 'mytable'})[0]
        table_rows = schedule_table.findAll('tr')
        games = []
        for idx, row in enumerate(table_rows):
            # skip the title row and header row
            if idx < 2:
                print 'index is', idx
                continue

            game_info = cls._process_schedule_row(row)
            if game_info is not None:
                games.append(game_info)

        return games

    @classmethod
    def _process_schedule_row(cls, row):
        """
        :param row:
        :return: [Datetime.date, String, Int, String, String, Int, Int, String]
        """
        tds = row.findAll('td')
        if len(tds) != 3:
            return None
        date_string = tds[0].get_text()
        game_date = datetime.strptime(date_string, '%m/%d/%Y').date()
        opp_link = tds[1].find('a')
        opp_text = tds[1].get_text()
        if opp_link is not None:
            opp_id, year_id = ScheduleScraper.url_to_teamid(opp_link['href'])
        else:
            opp_id, year_id = (None, None)
        opp, neutral_site = ScheduleScraper._parse_opp_string(opp_text)
        neutral = True if neutral_site else False
        outcome_string = tds[2].get_text()
        game_link = tds[2].find('a')
        if game_link is not None:
            game_url = game_link['href']
        else:
            game_url = None
        box_link, pbp_link = cls._parse_link(game_url)
        outcome, score, opp_score, num_ot = cls._parse_outcome(outcome_string)

        return [game_date, opp_id, opp, neutral, neutral_site, outcome, num_ot, score, opp_score, box_link, pbp_link]

    @staticmethod
    def _parse_outcome(outcome_string):
        s = outcome_string.strip()
        outcome = s[0]
        s = s[1:]
        assert(outcome in {'W', 'L'}, "unknown outcome: %s" % outcome)
        if 'OT' in s:
            ot_string = re.search('\(([0-9]OT)\)',s).group(1)
            num_ot = int(ot_string.replace('OT', ''))
            s = re.search('[^\(]+', s).group(0).strip()
        else:
            num_ot = 0
        scores = s.split('-')
        assert(len(scores) == 2, "bad outcome string: %s" % s)
        score, opp_score = scores[0].strip(), scores[1].strip()
        return outcome, int(score), int(opp_score), num_ot

    @staticmethod
    def url_to_teamid(url):
        s = url.split('index/')[-1]
        split1 = s.split('?')
        year_id = split1[0]
        team_id = split1[-1].split('=')[-1]

        return int(team_id), int(year_id)

    @staticmethod
    def _parse_opp_string(s):
        if '@' in s:
            splits = s.split('@')
            # if '@' is first character, then it is not neutral site
            if splits[0].strip() == '':
                opp, neutral_site = splits[1].strip(), None
            else:
                opp, neutral_site = splits[0].strip(), splits[1].strip()
        else:
            opp, neutral_site = s.strip(), None

        return opp, neutral_site

    @staticmethod
    def _parse_link(url):
        splits1 = url.split('index/')
        assert(len(splits1) == 2, "bad game link: %s" % url)
        splits2 = splits1[1].split('?')
        assert(len(splits2) == 2, "bad game link: %s" % url)

        game_id = splits2[0].strip()
        box_link = 'http://stats.ncaa.org/game/box_score/%s' % game_id
        pbp_link = 'http://stats.ncaa.org/game/play_by_play/%s' % game_id

        return box_link, pbp_link


