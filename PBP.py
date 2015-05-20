import psycopg2
import pandas as pd
import numpy as np
from collections import defaultdict
import time

CONN = psycopg2.connect(database="cbb", user="seth", password="abc123",
                        host="localhost", port="5432")
CUR = CONN.cursor()

class PBP(object):

    def __init__(self, raw_df):
        """
        INPUT: PBP, DATAFRAME
        OUTPUT: None

        Initialize a play-by-play processing object

        raw_df is a dataframe containing the sorted raw pbp data from the
            raw_pbp table

        NOTES: This object contains methods to extract many useful features
        from the pbp data. Many of these features rely on past and present 
        rows and some features rely on other features that rely on past and
        present rows. For this reason, the processing is done by iterating
        over the data several times, doing different extractions each time.

        It is much faster to iterate over a 2D numpy array than a Pandas DF.

        e.g. possession cannot be assigned on a foul without knowing whether
        it was an offensive foul or not. A foul is an offensive foul if a 
        turnover was committed by the team that fouled at the same point in
        time. It is not easy to process these things simultaneously, so we 
        first loop through to process offensive fouls, and then we loop again
        to process possession.
        """
        self.df = raw_df
        self.gameid = self.df.game_id.iloc[0]
        self.df = self.df[~self.df.play.isin({'ENTERS', 'LEAVES', 'DEADREB'})]
        self.point_vals = {'LUM': (2, 2), 'LUMS': (2, 0), 'JM': (2, 2), 'JMS': (2, 0),
             'TIM': (2, 2), 'TIMS': (2, 0), 'TPM': (3, 3), 'TPMS': (3, 0),
             'DM': (2, 2), 'DMS': (2, 0), 'FTM': (1, 1), 'FTMS': (1, 0)}
        self.numot = int((np.ceil(self.df.time.iloc[-1]) - 40) / 5.)
        self.periods = np.array([20] + range(40, 40 + 5*self.numot, 5))
        self.data = self.df.values
        self.col_index = {col: idx for idx, col in enumerate(self.df.columns)}
        self.field_goals = {'LUM', 'LUMS', 'JM', 'JMS', 'TIM', 'TIMS',
                          'TPM', 'TPMS', 'DM', 'DMS'}

    def point_value(self, play):
        """
        INPUT: PBP, STRING
        OUTPUT: TUPLE

        Return a tuple containing the point value and worth of a play.

        play is a string which describes the play (e.g. 'JM' - made jumper)

        value is the potential value if the shot had gone in and worth is 
        the actual point value the play results in. So, JM --> (2, 2) because
        a jumper is for two points and since it was made, it was worth two points.
        In contrast, JMS --> (2, 0) since it is a missed shot.
        """
        if play in self.point_vals:
            return self.point_vals[play]
        else:
            return (0, 0)

    def poss_time_error(self):
        """
        INPUT: PBP
        OUTPUT: INT

        Find the error between true game length in seconds and calculated
        game length from assigned possession times.
        """
        poss_time = self.df.poss_time.sum()
        true_poss_time = 2400 + self.numot * 300
        return true_poss_time - poss_time

    def add_col(self, col_name, col_data):
        """
        INPUT: PBP, STRING, LIST-LIKE
        OUTPUT: NONE

        Add a column to the data frame and update the column-index map
        """
        self.df[col_name] = col_data
        self.col_index = {col: idx for idx, col in enumerate(self.df.columns)}
        self.data = self.df.values

    def new_half(self, idx, half_type='all'):
        """
        INPUT: PBP, INT, STRING
        OUTPUT: BOOLEAN

        Check if the pbp row at the given index is the first row in a new
        half.

        idx indicates the row index
        half_type indicates whether to consider OT periods as halves
        """
        if idx == 0:
            return False
        time = self.data[idx][self.col_index['time']]
        ptime = self.data[idx - 1][self.col_index['time']]

        # checking for a new half is sort of expensive computationally, so
        # this quick check saves a lot of time
        if int(time) == int(ptime):
            return False
        new_halves = np.where(((time - self.periods) > 0) & ((ptime - self.periods) <= 0))[0]
        if new_halves.shape[0] == 0:
            return False
        if half_type == 'all':
            return True
        elif self.periods[new_halves[0]] == 20:
            return True
        else:
            return False

    def num_possessions(self):
        return np.sum(np.abs(np.diff(self.df.possession))) / 2.

    def points_off(self):
        """
        INPUT: PBP
        OUTPUT: NONE

        Loop through the pbp data and calculate points off of ___ variables.
        Additionally calculate possession times.

        e.g. we want to know when a team gets points off turnover or points
        off an offensive rebound (second chance points).
        """
        flags = {'second_chance': False, 'timeout_pts': False, 'turnover_pts': False}
        d = {'second_chance': [None]*self.data.shape[0],
             'timeout_pts': [None]*self.data.shape[0],
             'turnover_pts': [None]*self.data.shape[0]}
        prev_time = 0
        poss_times = []
        for idx, row in enumerate(self.data):
            if idx >= self.data.shape[0] - 1:
                # last row, so assign possession time
                poss_time = row[self.col_index['time']] - prev_time
                prev_time = row[self.col_index['time']]
            elif self.new_half(idx):
                # if first row in a new half, previous time should be 
                # the start of the half
                prev_time = round(row[self.col_index['time']] / 5.) * 5
                poss_time = row[self.col_index['time']] - prev_time
                prev_time = row[self.col_index['time']]
            elif self.new_half(idx + 1):
                # last row in the half, assign possession time
                poss_time = row[self.col_index['time']] - prev_time
                prev_time = row[self.col_index['time']]
            elif self.poss_change(idx):
                poss_time = row[self.col_index['time']] - prev_time
                prev_time = row[self.col_index['time']]
            else:
                poss_time = None

            if poss_time is not None:
                poss_time = int(round(poss_time*60))

            poss_times.append(poss_time)
            # ---------------------------------------------
            if self.new_half(idx):
                flags = {'second_chance': False, 'timeout_pts': False, 'turnover_pts': False}
            play = row[self.col_index['play']]
            if play == 'TURNOVER':
                for flag in flags:
                    if flags[flag]:
                        d[flag][idx] = 0
                        flags[flag] = False
            elif play in self.field_goals:
                for flag in flags:
                    if flags[flag]:
                        if row[self.col_index['and_one']] > 0:
                            d[flag][idx] = row[self.col_index['and_one']]
                        else:
                            d[flag][idx] = self.point_value(play)[1]
                        flags[flag] = False
            elif 'FT' in play:
                for flag in flags:
                    if flags[flag]:
                        d[flag][idx] = row[self.col_index['ft_total']]
                        flags[flag] = False

            if play == 'OREB':
                flags['second_chance'] = True
            elif play == 'TIMEOUT':
                flags['timeout_pts'] = True
            elif play == 'TURNOVER':
                flags['turnover_pts'] = True

        self.add_col('poss_time', poss_times)
        for col in d:
            self.add_col(col, d[col])

    def possession_and_one(self):
        """
        INPUT: PBP
        OUTPUT: NONE

        Loop through the data and assign possession to each play. Also,
        calculate whether a shot was an and one and how much that and one
        was worth
        """
        ptime = -1
        and_ones = [None]*self.data.shape[0]
        d = {}
        d[1] = {'shot_index': -1, 'shot_worth': 0, 'ft_count': 0,
                 'ft_total': 0}
        d[0] = {'shot_index': -1, 'shot_worth': 0, 'ft_count': 0,
                 'ft_total': 0}
        flip = {'BLOCK', 'DREB', 'STEAL'}
        same = {'ASSIST', 'TPM', 'TPMS', 'FTM', 'FTMS', 'LUM', 'LUMS',
                'JM', 'JMS', 'DM', 'DMS', 'TIM', 'TIMS', 'TURNOVER',
                'OREB'}
        possessions = np.zeros(self.df.shape[0])
        timeouts = []
        for idx, row in enumerate(self.data):
            play = row[self.col_index['play']]
            teamid = row[self.col_index['teamid']]
            if row[self.col_index['time']] > ptime:
                # process last time chunk
                if d[1]['shot_index'] != -1 and d[1]['ft_count'] == 1:
                    and_ones[d[1]['shot_index']] = d[1]['shot_worth'][1] + d[1]['ft_total']
                if d[0]['shot_index'] != -1 and d[0]['ft_count'] == 1:
                    and_ones[d[0]['shot_index']] = d[0]['shot_worth'][1] + d[0]['ft_total']

                d[1] = {'shot_index': -1, 'shot_worth': 0, 'ft_count': 0,
                         'ft_total': 0}
                d[0] = {'shot_index': -1, 'shot_worth': 0, 'ft_count': 0,
                         'ft_total': 0}

            if play in {'LUM', 'DM', 'JM', 'TIM', 'TPM'}:
                d[teamid]['shot_index'] = idx
                d[teamid]['shot_worth'] = self.point_value(play)
            elif 'FT' in play:
                d[teamid]['ft_count'] += 1
                d[teamid]['ft_total'] += 1
            # --------------------------------------------------------
            if play in same:
                possessions[idx] = teamid
                last_known_possession = teamid
            elif play in flip:
                possessions[idx] = abs(teamid - 1)
                last_known_possession = abs(teamid - 1)
            elif play == 'FOUL':
                if row[self.col_index['charge']] == 1:
                    possessions[idx] = teamid
                    last_known_possession = teamid
                else:
                    possessions[idx] = abs(teamid - 1)
                    last_known_possession = abs(teamid - 1)
            elif play == 'TIMEOUT':
                timeouts.append(j)
            elif play == 'TREB':
                possesions[idx] = last_known_possession

            if play != 'TIMEOUT' and len(timeouts) > 0:
                for timeout in timeouts:
                    possessions[timeout] = possessions[idx]
                timeouts = []
            if play == 'TIMEOUT':
                timeouts.append(idx)
            ptime = row[self.col_index['time']]

        self.add_col('and_one', and_ones)
        self.add_col('possession', possessions)

    def off_foul(self, j, row, status_dict):
        """
        INPUT: PBP, INT, SERIES, DICT
        OUTPUT: INT

        Determine if a foul was offensive. 

        j is the row index
        row is a series containing the row data
        status_dict is a dictionary which keeps track of fouls and turnovers
            that occur at the same time
        """
        if row[self.col_index['play']] == 'FOUL':
            status_dict[row[self.col_index['teamid']]]['foul'] = j
        elif row[self.col_index['play']] == 'TURNOVER':
            status_dict[row[self.col_index['teamid']]]['turnover'] = True

        for team in status_dict:
            if status_dict[team]['foul'] != -1 and status_dict[team]['turnover']:
                return status_dict[team]['foul']

    def off_fouls(self):
        """
        INPUT: PBP
        OUTPUT: NONE

        Loop through the data to extract offensive fouls, assist attributes,
        turnovers that were stolen, and shots that were blocked.
        """
        ptime = -1
        charges = np.array([None]*self.data.shape[0])
        charge_indices = []
        assisted = [None]*self.data.shape[0]
        recipients = [None]*self.data.shape[0]
        assisted_plays = [None]*self.data.shape[0]
        blocked = [None]*self.data.shape[0]
        stolen = [None]*self.data.shape[0]
        fts = {1: {'indices': [], 'count': 0, 'total': 0}, 
               0: {'indices': [], 'count': 0, 'total': 0}}
        ft_total = np.zeros(self.df.shape[0])
        ft_count = np.zeros(self.df.shape[0])
        off_fouls = {0: {'foul': -1, 'turnover': False},
                     1: {'foul': -1, 'turnover': False}}
        home_fouls = np.zeros(self.df.shape[0])
        away_fouls = np.zeros(self.df.shape[0])
        hfouls = 0
        afouls = 0
        for idx, row in enumerate(self.data):
            if self.new_half(idx, half_type='half'):
                hfouls = 0
                afouls = 0
            if row[self.col_index['play']] == 'FOUL':
                if row[self.col_index['teamid']] == 1:
                    hfouls += 1
                else:
                    afouls += 1
            home_fouls[idx] = hfouls
            away_fouls[idx] = afouls
            # ----------------------------------------
            if row[self.col_index['play']] == 'STEAL':
                if idx == 0:
                    pass
                elif self.data[idx - 1][self.col_index['play']] == 'TURNOVER':
                    stolen[idx - 1] = True
            # ----------------------------------------
            if row[self.col_index['play']] == 'BLOCK':
                if idx == 0:
                    pass
                elif self.data[idx - 1][self.col_index['play']] in self.field_goals:
                    blocked[idx - 1] = True
            # ----------------------------------------
            if row[self.col_index['play']] == 'ASSIST':
                if idx == 0:
                    pass
                elif self.data[idx - 1][self.col_index['play']] in self.field_goals:
                    assisted[idx - 1] = True
                    rfirst_name = self.data[idx - 1][self.col_index['first_name']]
                    rlast_name = self.data[idx - 1][self.col_index['last_name']]
                    recipients[idx] = rfirst_name + ' ' + rlast_name
                    assisted_plays[idx] = self.data[idx - 1][self.col_index['play']]
            # ---------------------------------
            if row[self.col_index['time']] > ptime:
                off_fouls = {0: {'foul': -1, 'turnover': False},
                             1: {'foul': -1, 'turnover': False}}
                for team in fts:
                    for index in fts[team]['indices']:
                        ft_total[index] = fts[team]['total']
                        ft_count[index] = fts[team]['count']
                fts = {1: {'indices': [], 'count': 0, 'total': 0}, 
                       0: {'indices': [], 'count': 0, 'total': 0}}

            charge_index = self.off_foul(idx, row, off_fouls)
            if charge_index is not None:
                charge_indices.append(charge_index)
            if 'FT' in row[self.col_index['play']]:
                fts[row[self.col_index['teamid']]]['total'] += self.point_value(row[self.col_index['play']])[1]
                fts[row[self.col_index['teamid']]]['count'] += 1
                fts[row[self.col_index['teamid']]]['indices'].append(idx)

            ptime = row[self.col_index['time']]

        charges[charge_indices] = True

        self.add_col('charge', charges)
        self.add_col('home_fouls', home_fouls)
        self.add_col('away_fouls', away_fouls)
        self.add_col('assist_play', assisted_plays)
        self.add_col('blocked', blocked)
        self.add_col('stolen', stolen)
        self.add_col('assisted', assisted)
        self.add_col('recipient', recipients)
        self.add_col('ft_total', ft_total)
        self.add_col('ft_count', ft_count)

    def poss_change(self, idx):
        """
        INPUT: PBP, INT
        OUTPUT: BOOLEAN

        Determine if the event at the given index caused a possession change.
        """
        if idx >= self.data.shape[0] - 1:
            return False 
        elif self.data[idx][self.col_index['possession']] != \
            self.data[idx + 1][self.col_index['possession']]:
            return True
        else:
            return False

    def poss_time_full(self):
        """
        INPUT: PBP
        OUTPUT: NONE

        Loop backwards through the data to assign a possession time to 
        every single row. Each event will have the length of the possession
        on which that event occurred.
        """
        poss_time = self.data[-1][self.col_index['poss_time']]
        poss_time_full = np.zeros(self.data.shape[0])
        for i in xrange(self.data.shape[0] - 1, -1, -1):
            if np.isnan(self.data[i][self.col_index['poss_time']]):
                poss_time_full[i] = poss_time
            else:
                poss_time = self.data[i][self.col_index['poss_time']]
                poss_time_full[i] = poss_time

        self.add_col('poss_time_full', poss_time_full.astype(int))

    def sql_convert(self):
        """
        INPUT: PBP
        OUTPUT: NONE

        Reorder the dataframe columns to match the PostgreSQL table
        """
        cols = ['game_id', 'id', 'team', 'teamid', 'time', 'first_name',
                'last_name', 'play', 'hscore', 'ascore', 'possession',
                'poss_time_full', 'poss_time', 'home_fouls', 'away_fouls',
                'second_chance', 'timeout_pts', 'turnover_pts', 'and_one',
                'blocked', 'stolen', 'assisted', 'assist_play', 'recipient',
                'charge']
        self.df = self.df[cols]

    def process(self):
        """
        INPUT: PBP
        OUTPUT: NONE

        Execute the various processing loops to process the data.
        """
        # some games have data with missing plays, so skip them
        if pd.isnull(self.df.play).sum() > 0:
            return None
        self.off_fouls()
        self.possession_and_one()
        self.points_off()
        self.poss_time_full()
        self.sql_convert()
        return self.df

def data_convert(values):
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

def insert_data(values):
    values = data_convert(values)
    q =  """ INSERT INTO pbp 
                (game_id, pbp_id, team, teamid, time, first_name, last_name,
                 play, hscore, ascore, possession, poss_time_full,
                 poss_time, home_fouls, away_fouls, second_chance,
                 timeout_pts, turnover_pts, and_one, blocked, stolen,
                 assisted, assist_play, recipient, charge) 
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
         """

    CUR.executemany(q, values)
    CONN.commit()


if __name__ == '__main__':

    q = """ SELECT *
            FROM raw_pbp
            WHERE game_id
            IN
                (SELECT DISTINCT(game_id)
                FROM raw_pbp
                WHERE game_id NOT IN (SELECT DISTINCT(game_id) FROM pbp)
                LIMIT 10)
            ORDER BY id
        """
    df = pd.read_sql(q, CONN)
    for i, game_id in enumerate(df.game_id.unique()):
        subdf = df[df.game_id == game_id]
        pbp = PBP(subdf)
        pbpdf = pbp.process()
        if pbpdf is None:
            continue
        if i == 0:
            bigdf = pbpdf
        else:
            bigdf = pd.concat([bigdf, pbpdf])
        print i, game_id, pbp.poss_time_error()

    insert_data(bigdf.values)

    # print bigdf.shape
    # start = time.time()
    # pbp = PBP(pd.read_sql("""SELECT * FROM ncaa_pbp WHERE game_id=3652267""", CONN))
    # middle = time.time()
    # pbp.process()
    # end = time.time()
    # print middle - start, end - middle