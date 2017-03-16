import re
import string


class NCAAStatsUtil(object):
    """
    A collection of functions that handle scraping tasks specific to
    the stats.ncaa.org domain.
    """

    # static variables
    stats_ncaa_year_map = {10440: 2010, 10260: 2009, 10740: 2011, 11220: 2012, 11540: 2013, 12020: 2014}
    box_link_base = 'http://stats.ncaa.org/game/box_score/'
    pbp_link_base = 'http://stats.ncaa.org/game/play_by_play/'
    box_columns = ['game_id', 'Team', 'team_id', 'first_name', 'last_name',
                   'Pos','Min','FGM', 'FGA', '3FG', '3FGA', 'FT', 'FTA',
                   'PTS', 'Off Reb', 'Def Reb', 'Tot Reb', 'AST', 'TO', 'ST',
                   'BLKS', 'Fouls']
    col_map = {'Min': 'Min', 'MP': 'Min', 'Tot Reb': 'Tot Reb',
               'Pos': 'Pos', 'FGM': 'FGM', 'FGA': 'FGA',
               '3FG': '3FG', '3FGA': '3FGA','FT': 'FT',
               'FTA': 'FTA', 'PTS': 'PTS', 'Off Reb': 'Off Reb',
               'ORebs': 'Off Reb', 'Def Reb': 'Def Reb',
               'DRebs': 'Def Reb', 'BLK': 'BLKS', 'BLKS': 'BLKS',
               'ST': 'ST', 'STL': 'ST', 'Player': 'Player',
               'AST': 'AST', 'TO': 'TO', 'Fouls': 'Fouls',
               'Team': 'Team', 'game_id': 'game_id', 'Time': 'Time',
               'team_id': 'team_id'}

    stat_list = ['MADE', 'MISSED', 'REBOUND', 'ASSIST', 'BLOCK',
                 'STEAL', 'TURNOVER', 'FOUL', 'TIMEOUT', 'ENTERS',
                 'LEAVES']
    shot_code_map = {'THREE POINT': 'TPM', 'FREE THROW': 'FTM',
                     'LAYUP': 'LUM', 'TWO POINT': 'JM',
                     'DUNK': 'DM', 'TIP': 'TIM'}
    rebound_code_map = {'OFFENSIVE': 'OREB', 'TEAM': 'TREB',
                        'DEFENSIVE': 'DREB', 'DEADBALL': 'DEADREB'}
    ALIASES = {101: {'Cal St. Northridge','CSUN'},
               460: {'UNC Wilmington','UNCW'},
               660: {'SIU Edwardsville','SIUE'}}

    @staticmethod
    def convert_ncaa_year_code(val):
        """Swap between the stats.ncaa.org year code and the actual year."""
        code_to_year = NCAAStatsUtil.stats_ncaa_year_map
        year_to_code = {v: k for k, v in code_to_year.iteritems()}
        if val in code_to_year:
            return code_to_year[val]
        elif val in year_to_code:
            return year_to_code[val]
        else:
            return None

    @staticmethod
    def all_years():
        """All current available years for stats.ncaa.org"""
        return [v for k, v in NCAAStatsUtil.stats_ncaa_year_map.iteritems()]

    @staticmethod
    def stats_link(game_id, link_type='box'):
        """Construct box stats and play-by-play stats links from stats.ncaa.org game id"""
        assert link_type in {'box', 'pbp'}, "invalid link type: %s" % link_type
        if link_type == 'box':
            link = 'http://stats.ncaa.org/game/box_score/%s' % game_id
        elif link_type == 'pbp':
            link = 'http://stats.ncaa.org/game/play_by_play/%s' % game_id

        return link

    @staticmethod
    def get_team_id(url):
        """Extract the stats.ncaa.org team id from a url to the team's page"""
        pattern = "org_id=[0-9]+"
        match = re.search(pattern, url)
        if match is not None:
            return int(match.group().split("org_id=")[-1])
        else:
            return None

    @staticmethod
    def parse_outcome(outcome_string):
        """Extract useful parts of an outcome string like 'L 80-82 (2OT)'"""
        if 'W' not in outcome_string and 'L' not in outcome_string:
            return None, None, None, None
        s = outcome_string.strip()
        outcome = s[0]
        assert outcome in {'W', 'L'}, "unknown outcome: %s" % outcome

        s = s[1:]
        if 'OT' in s:
            ot_string = re.search('\(([0-9]OT)\)',s).group(1)
            num_ot = int(ot_string.replace('OT', ''))
            s = re.search('[^\(]+', s).group(0).strip()
        else:
            num_ot = 0

        scores = s.split('-')
        assert len(scores) == 2, "bad outcome string: %s" % s
        score, opp_score = scores[0].strip(), scores[1].strip()

        return outcome, int(score), int(opp_score), num_ot

    @staticmethod
    def parse_opp_string(opp_string):
        """Extract useful parts of the opponent column of a team's schedule"""
        if '@' in opp_string:
            splits = opp_string.split('@')
            # if '@' is first character, then it is not neutral site
            if splits[0].strip() == '':
                opp, neutral_site = splits[1].strip(), None
                loc = 'A'
            else:
                opp, neutral_site = splits[0].strip(), splits[1].strip()
                loc = 'N'
        else:
            opp, neutral_site = opp_string.strip(), None
            loc = 'H'

        return opp, neutral_site, loc

    @staticmethod
    def parse_game_link(url):
        """
        Extract game id from the game link url
        Note: the game link url is not the same as the box_score or play_by_play urls
        """
        # TODO: use regex
        splits1 = url.split('index/')
        assert len(splits1) == 2, "bad game link: %s" % url
        splits2 = splits1[1].split('?')
        assert len(splits2) == 2, "bad game link: %s" % url

        game_id = splits2[0].strip()

        return int(game_id)

    @staticmethod
    def clean_string(s):
        """Get only printable characters from a string or unicode type"""
        if type(s) == str or type(s) == unicode:
            clean = filter(lambda char: char in string.printable, s)
            return str(clean)
        else:
            return str(s)

    @staticmethod
    def parse_name(full_name):
        """Get first and last name from the name column of box stats table"""
        full_name = str(full_name)
        parts = full_name.split(",")
        if len(parts) == 2:
            first_name, last_name = parts[1].strip(), parts[0].strip()
        else:
            first_name, last_name = full_name.strip(), ''

        return first_name, last_name

    @staticmethod
    def parse_stats_link(url):
        """Get game id from the box_score or play_by_play urls"""
        splits = url.split("/")
        if len(splits) > 0:
            game_id = splits[-1]

        return int(game_id)

    @staticmethod
    def time_to_dec(time_string, half):
        """
        INPUT: NCAAScraper, STRING, INT
        OUTPUT: FLOAT

        Convert a time string 'MM:SS' remaining in a half to a
        float representing absolute time elapsed

        time_string is a string in the form 'MM:SS'
        half is an integer representing which half the game is in (>1 is OT)
        """
        # some rows may not have valid time strings
        # TODO: use regex
        if ':' not in time_string:
            return -1

        minutes, seconds = time_string.split(':')
        t = float(minutes) + float(seconds) / 60.

        if half == 0:
            return 20. - t
        if half == 1:
            return 40. - t
        else:
            return (40. + (half - 1) * 5.) - t

    @staticmethod
    def string_to_stat(stat_string):
        """
        INPUT: NCAAScraper, STRING
        OUTPUT: STRING

        Convert a string which describes an action into a unique encoded
        string representing that action

        stat_string is a string representing the action (e.g. 'missed dunk')
        """
        stat_string = stat_string.upper()
        stat = None
        for st in NCAAStatsUtil.stat_list:
            if st in stat_string:
                stat = st
                break

        if stat == 'MADE' or stat == 'MISSED':
            for shot in NCAAStatsUtil.shot_code_map:
                if shot in stat_string:
                    if stat == 'MISSED':
                        return NCAAStatsUtil.shot_code_map[shot] + 'S'
                    else:
                        return NCAAStatsUtil.shot_code_map[shot]
        elif stat == 'REBOUND':
            for rebound in NCAAStatsUtil.rebound_code_map:
                if rebound in stat_string:
                    return NCAAStatsUtil.rebound_code_map[rebound]
        else:
            return stat

    @staticmethod
    def split_play(play_string):
        """
        INPUT: NCAAScraper, STRING
        OUTPUT: STRING, STRING, STRING

        Split the raw full play string into a play, first name, and last name

        play_string describes the play (e.g. DOE, JOHN made jumper)

        WARNING: This will not work when the player's name is not in all caps.
        Some of the older pbp pages do not use this pattern and so the player's
        name will not be extracted properly
        """

        # name is everything until a non-capitalized letter or other invalid character is found
        pattern = r"^[A-Z,\s'\.-]+\b"
        rgx = re.match(pattern, play_string)
        if rgx is None:
            player = ''
        else:
            player = rgx.group(0).strip()
        play = play_string.replace(player, '').strip()
        # TODO: use parse_name function
        if player == 'TEAM' or player == 'TM':
            last_name, first_name = '', 'TEAM'
        elif ',' not in player:
            print('Bad player string', player)
            last_name, first_name = ('', '')
        else:
            splits = player.split(',')
            last_name, first_name = splits[0], splits[1]

        return play, first_name, last_name