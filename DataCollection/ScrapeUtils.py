from datetime import datetime, date, timedelta
import DataCollection.StatsNCAAUtil as ncaa_util

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



