import psycopg2
import pandas as pd
import numpy as np
from collections import defaultdict
import time

CONN = psycopg2.connect(database="cbb", user="seth", password="abc123",
                        host="localhost", port="5432")
CUR = CONN.cursor()

class PBP(object):

    def __init__(self, gameid):
        q = """ SELECT *
                FROM ncaa_pbp
                WHERE game_id=%s
            """ % gameid
        self.gameid = gameid
        self.df = pd.read_sql(q, CONN)
        self.df = self.df[~self.df.play.isin({'ENTERS', 'LEAVES', 'DEADREB'})]
        self.df['tdiff'] = self.df.time.diff()
        a = self.df.tdiff[(self.df.tdiff > 0) | (np.isnan(self.df.tdiff))].index.values
        b = np.roll(a, -1)
        b[-1] = self.df.shape[0] - 1
        self.time_indices = zip(a, b)
        self.shot_list = {'LUM', 'LUMS', 'JM', 'JMS', 'TIM', 'TIMS',
                          'TPM', 'TPMS', 'DM', 'DMS'}

    def point_value(self, play):
        d = {'LUM': (2, 2), 'LUMS': (2, 0), 'JM': (2, 2), 'JMS': (2, 0),
             'TIM': (2, 2), 'TIMS': (2, 0), 'TPM': (3, 3), 'TPMS': (3, 0),
             'DM': (2, 2), 'DMS': (2, 0), 'FTM': (1, 1), 'FTMS': (1, 0)}
        if play in d:
            return d[play]
        else:
            return (0, 0)

    def numot(self):
        return int((np.ceil(self.df.time.iloc[-1]) - 40) / 5.)

    def insert_half(self):
        row = {'game_id': self.gameid, 'team': 'HALF', 'first_name': 'HALF',
               'play': 'HALF', 'hscore': 0, 'ascore': 0}
        row['time'] = 20

    def new_half(self, idx, half_type='all'):
        if idx == 0:
            return False
        periods = np.array([20] + range(40, 40 + 5*self.numot(), 5))
        time = self.df.time.iloc[idx]
        ptime = self.df.time.iloc[idx - 1]
        new_halves = np.where(((time - periods) > 0) & ((ptime - periods) <= 0))[0]
        if new_halves.shape[0] == 0:
            return False
        if half_type == 'all':
            return True
        elif periods[new_halves[0]] == 20:
            return True
        else:
            return False

    def update_fouls(self, idx, home_fouls, away_fouls):
        row = self.df.iloc[idx]
        if self.new_half(idx, half_type='half'):
            home_fouls = 0
            away_fouls = 0
        if row.play == 'FOUL':
            if row.teamid:
                home_fouls += 1
            else:
                away_fouls += 1

        return home_fouls, away_fouls

    def num_possessions(self):
        return np.sum(np.abs(np.diff(self.df.possession))) / 2.

    # def and_one(self, idx):
    #     # check for an and one each time there is a shot
    #     if self.df.iloc[idx].play not in self.shot_list:
    #         return 0

    #     j = idx + 1
    #     ft_count = 0
    #     ft_points = 0
    #     while True:
    #         if j >= self.df.shape[0]:
    #             break
    #         elif self.df.iloc[j].time > self.df.iloc[idx].time:
    #             break
    #         if (self.df.iloc[idx].teamid == self.df.iloc[j].teamid) and \
    #            ('FT' in self.df.iloc[j].play):
    #            ft_count += 1
    #            ft_points += self.point_value(self.df.iloc[j].play)[1]
    #         j += 1

    #     if ft_count == 1:
    #         return self.point_value(self.df.iloc[idx].play)[1] + ft_points
    #     else:
    #         return 0

    # def possession(self, idx):
    #     row = self.df.iloc[idx]
    #     # uncertain scenarios: foul (offensive?), deadreb, teamreb,
    #     # enters/leaves, 
    #     flip = {'BLOCK', 'DREB', 'STEAL'}
    #     same = {'ASSIST', 'TPM', 'TPMS', 'FTM', 'FTMS', 'LUM', 'LUMS',
    #             'JM', 'JMS', 'DM', 'DMS', 'TIM', 'TIMS', 'TURNOVER'}
    #     if row.play in flip:
    #         return abs(row.teamid - 1)
    #     elif row.play in same:
    #         return row.teamid
    #     elif row.play == 'FOUL':
    #         if self.is_offensive_foul(idx):
    #             # possession is same for offensive foul
    #             return row.teamid
    #         else:
    #             return abs(row.teamid - 1)
    #     elif row.play in {'TIMEOUT', 'ENTERS', 'LEAVES'}:
    #         # for timeouts, possession should go to whoever has possession
    #         # out of the timeout. Loop until you find that play, then
    #         # infer possession
    #         j = idx + 1
    #         while True:
    #             if j > 1000:
    #                 break
    #             if self.df.iloc[j].play in {'TIMEOUT', 'ENTERS', 'LEAVES'}:
    #                 j += 1
    #             elif j > self.df.shape[0]:
    #                 return -1
    #             else:
    #                 possession = self.possession(j)
    #                 return possession
    #     else:
    #         return -1

    # def is_offensive_foul(self, idx):
    #     if idx == 0:
    #         return False
    #     row = self.df.iloc[idx]
    #     prow = self.df.iloc[idx - 1]
    #     if (prow.play == 'TURNOVER') and (prow.time == row.time) \
    #         and (prow.teamid == row.teamid):
    #         return True

    # when a turnover happens
    def ft_points(self, idx):
        j = idx
        ft_total = 0
        while True:
            if j >= self.df.shape[0]:
                break
            row = self.df.iloc[j]
            if row.time > self.df.iloc[idx].time:
                break
            elif 'FT' in row.play:
                ft_total += self.point_value(row.play)[1]
            j += 1

        return ft_total

    def second_chance(self, idx, and_one=0):
        row = self.df.iloc[idx]
        if row.play in self.shot_list:
            # is it an and one?
            if and_one > 0:
                return and_one
            else:
                return self.point_value(row.play)[1]
        elif row.play == 'TURNOVER':
            return 0
        elif 'FT' in row.play:
            return self.ft_points(idx)
        else:
            return -1

    # def points_off(self, idx, and_one=0):
    #     row = self.df.iloc[idx]
    #     if row.play in self.shot_list:
    #         # is it an and one?
    #         if and_one > 0:
    #             return and_one
    #         else:
    #             return self.point_value(row.play)[1]
    #     elif row.play == 'TURNOVER':
    #         return 0
    #     elif 'FT' in row.play:
    #         return self.ft_points(idx)
    #     else:
    #         return -1

    def points_off(self):
        flags = {'second_chance': False, 'timeout': False, 'turnover': False}
        d = {'second_chance': np.one(self.df.shape[0])*-1,
             'timeout': np.one(self.df.shape[0])*-1,
             'turnover': np.one(self.df.shape[0])*-1}
        j = 0
        for idx, row in self.df.iterrows():
            if row.play == 'TURNOVER':
                for flag in flags:
                    if flags[flag]:
                        d[flag][j] = 0
            elif row.play in self.shot_list:
                for flag in flags:
                    if flags[flag]:
                        d[flag][j] = self.point_value(row.play)
            elif 'FT' in row.play:
                # what here?
                pass

            if row.play == 'OREB':
                flags['second_chance'] = True
            elif row.play == 'TIMEOUT':
                flags['timeout'] = True
            elif row.play == 'TURNOVER':
                flags['turnover'] = True

            j += 1

    def possession(self):
        flip = {'BLOCK', 'DREB', 'STEAL'}
        same = {'ASSIST', 'TPM', 'TPMS', 'FTM', 'FTMS', 'LUM', 'LUMS',
                'JM', 'JMS', 'DM', 'DMS', 'TIM', 'TIMS', 'TURNOVER',
                'OREB'}
        possessions = np.zeros(self.df.shape[0])
        timeouts = []
        j = 0
        last_known_possession = 0
        for idx, row in self.df.iterrows():
            if row.play in same:
                possessions[j] = row.teamid
                last_known_possession = row.teamid
            elif row.play in flip:
                possessions[j] = abs(row.teamid - 1)
                last_known_possession = abs(row.teamid - 1)
            elif row.play == 'FOUL':
                if row.charge == 1:
                    possessions[j] = row.teamid
                    last_known_possession = row.teamid
                else:
                    possessions[j] = abs(row.teamid - 1)
                    last_known_possession = abs(row.teamid - 1)
            elif row.play == 'TIMEOUT':
                timeouts.append(j)
            elif row.play == 'TREB':
                possesions[j] = last_known_possession

            if row.play != 'TIMEOUT' and len(timeouts) > 0:
                for timeout in timeouts:
                    possessions[timeout] = possessions[j]
                timeouts = []

            j += 1
        self.df['possession'] = possessions

    def and_one(self):
        ptime = -1
        j = 0
        and_ones = np.zeros(self.df.shape[0])
        d = {}
        d[1] = {'shot_index': -1, 'shot_worth': 0, 'ft_count': 0,
                 'ft_total': 0}
        d[0] = {'shot_index': -1, 'shot_worth': 0, 'ft_count': 0,
                 'ft_total': 0}
        for idx, row in self.df.iterrows():
            if row.time > ptime:
                # process last time chunk
                if d[1]['shot_index'] != -1 and d[1]['ft_count'] == 1:
                    and_ones[d[1]['shot_index']] = d[1]['shot_worth'][1] + d[1]['ft_total']
                if d[0]['shot_index'] != -1 and d[0]['ft_count'] == 1:
                    and_ones[d[0]['shot_index']] = d[0]['shot_worth'][1] + d[0]['ft_total']

                d[1] = {'shot_index': -1, 'shot_worth': 0, 'ft_count': 0,
                         'ft_total': 0}
                d[0] = {'shot_index': -1, 'shot_worth': 0, 'ft_count': 0,
                         'ft_total': 0}

            if row.play in {'LUM', 'DM', 'JM', 'TIM', 'TPM'}:
                d[row.teamid]['shot_index'] = j
                d[row.teamid]['shot_worth'] = self.point_value(row.play)
            elif 'FT' in row.play:
                d[row.teamid]['ft_count'] += 1
                d[row.teamid]['ft_total'] += 1

            ptime = row.time
            j += 1

        self.df['and_one'] = and_ones

    def off_fouls(self):
        ptime = -1
        charges = np.zeros(self.df.shape[0])
        j = 0
        for idx, row in self.df.iterrows():
            if row.time > ptime:
                fouls = {0: [], 1: []}
                turnovers = {0: [], 1: []}
            if row.play == 'FOUL':
                fouls[row.teamid].append(j)
            elif row.play == 'TURNOVER':
                turnovers[row.teamid].append(j)

            for team in [0, 1]:
                if len(fouls[team]) > 0 and len(turnovers[team]) > 0:
                    for foul in fouls[team]:
                        charges[foul] = 1
            ptime = row.time
            j += 1

        self.df['charge'] = charges

    def process(self):
        d = defaultdict(list)
        home_fouls = 0
        away_fouls = 0
        flags = {'second_chance_pts': False, 'timeout_pts': False,
                 'turnover_pts': False}
        self.off_fouls()
        self.possession()
        self.and_one()
        # self.df['possession'] = self.df.apply(poss, 1)
        # plays = {0: [], 1:[]}
        # for idx, row in self.df.iterrows():
        #     current_time_df = self.df[self.df.time==row.time]


        # for col in d:
        #     self.df[col] = d[col]

def poss(row):
    shot_list = {'LUM', 'LUMS', 'JM', 'JMS', 'TIM', 'TIMS',
                          'TPM', 'TPMS', 'DM', 'DMS', 'FTM', 'FTMS'}
    if row.play in shot_list:
        return row.teamid
    elif row.play in {'ASSIST', 'TURNOVER', 'OREB'}:
        return row.teamid
    elif row.play in {'DREB', 'STEAL', 'BLOCK'}:
        return abs(1 - row.teamid)
    else:
        return -1

if __name__ == '__main__':
    start = time.time()
    pbp = PBP(3669104)
    middle = time.time()
    # print pbp.df.head()
    pbp.process()
    end = time.time()
    print middle - start, end - middle

""" SELECT a.*, b.*
    FROM ncaa_pbp a
    JOIN ncaa_pbp b
    ON a.game_id=b.game_id
    AND a.play='FOUL'
    AND b.play='TURNOVER'
    AND a.time=b.time
    AND a.teamid=b.teamid
    AND a.game_id=3669104
"""