from DataCollection.DB import DB
from DataCollection.NCAAStatsUtil import NCAAStatsUtil as ncaa_util

CONN = DB.conn
CUR = CONN.cursor()

def test_convert_ncaa_year_code():
    convert2012 = ncaa_util.convert_ncaa_year_code(2012)
    assert ncaa_util.convert_ncaa_year_code(convert2012) == 2012

def test_stats_link():
    game_id = 170096
    box_link = ncaa_util.stats_link(game_id, 'box')
    pbp_link = ncaa_util.stats_link(game_id, 'pbp')

    assert str(game_id) in box_link
    assert str(game_id) in pbp_link
    assert 'box_score' in box_link
    assert 'play_by_play' in pbp_link

def test_get_team_id():
    url = 'http://stats.ncaa.org/team/index/10440?org_id=649otherstuff'
    assert ncaa_util.get_team_id(url) == 649
    url = 'http://stats.ncaa.org/team/index/10440?org_id=otherstuff'
    assert ncaa_util.get_team_id(url) is None

def test_parse_outcome():
    outcome = 'W 78 - 70 (4OT)'
    output = ncaa_util.parse_outcome(outcome)
    assert output[0] == 'W'
    assert output[1] == 78
    assert output[2] == 70
    assert output[3] == 4

    outcome = 'L 93 - 42 '
    output = ncaa_util.parse_outcome(outcome)
    assert output[0] == 'L'
    assert output[1] == 93
    assert output[2] == 42
    assert output[3] == 0

def test_parse_opp_string():
    opp_string = '@ Ohio St.'
    output = ncaa_util.parse_opp_string(opp_string)
    assert output[0] == 'Ohio St.'
    assert output[1] == None
    assert output[2] == 'A'

    opp_string = 'Austin Peay @ Sears Centre Hoffman Estates, IL'
    output = ncaa_util.parse_opp_string(opp_string)
    assert output[0] == 'Austin Peay'
    assert output[1] == 'Sears Centre Hoffman Estates, IL'
    assert output[2] == 'N'

    opp_string = 'IUPUI'
    output = ncaa_util.parse_opp_string(opp_string)
    assert output[0] == 'IUPUI'
    assert output[1] == None
    assert output[2] == 'H'

def test_parse_game_link():
    url = 'http://stats.ncaa.org/game/index/180391?org_id=514'
    output = ncaa_util.parse_game_link(url)
    assert output == 180391

def test_clean_string():
    assert ncaa_util.clean_string(u'aisd90\xc2') == 'aisd90'

