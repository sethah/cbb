import re

class NCAAStatsUtil(object):

    # static variables
    stats_ncaa_year_map = {10440: 2010, 10260: 2009, 10740: 2011, 11220: 2012, 11540: 2013, 12020: 2014}

    @staticmethod
    def convert_ncaa_year_code(val):
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
        return [v for k, v in NCAAStatsUtil.stats_ncaa_year_map.iteritems()]

    @staticmethod
    def stats_link(game_id, link_type='box'):
        assert link_type in {'box', 'pbp'}, "invalid link type: %s" % link_type
        if link_type == 'box':
            link = 'http://stats.ncaa.org/game/box_score/%s' % game_id
        elif link_type == 'pbp':
            link = 'http://stats.ncaa.org/game/play_by_play/%s' % game_id

        return link

    @staticmethod
    def get_team_id(url):
        pattern = "org_id=[0-9]+"
        match = re.search(pattern, s).group()
        if match is not None:
            return int(url.split("org_id=")[-1])
        else:
            return None

    @staticmethod
    def parse_outcome(outcome_string):
        if 'W' not in outcome_string and 'L' not in outcome_string:
            return None, None, None, None
        s = outcome_string.strip()
        outcome = s[0]
        assert(outcome in {'W', 'L'}, "unknown outcome: %s" % outcome)

        s = s[1:]
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
    def parse_opp_string(opp_string):
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
        # TODO: use regex
        splits1 = url.split('index/')
        assert(len(splits1) == 2, "bad game link: %s" % url)
        splits2 = splits1[1].split('?')
        assert(len(splits2) == 2, "bad game link: %s" % url)

        game_id = splits2[0].strip()

        return game_id