import numpy as np
from datetime import datetime, timedelta, date
from itertools import izip
import psycopg2
import pandas as pd

conn = psycopg2.connect(database="cbb", user="seth", password="abc123",
                        host="localhost", port="5432")
cur = conn.cursor()

q = """ select a.*, b.*
    from teams b
    left outer join teams2 a
    on regexp_replace(a.ncaa,'[^a-zA-Z]', '', 'g')=regexp_replace(b.team_name,'[^a-zA-Z]', '', 'g')
    or regexp_replace(a.espn_name,'[^a-zA-Z]', '', 'g')=regexp_replace(b.team_name,'[^a-zA-Z]', '', 'g')
    or regexp_replace(a.kenpom,'[^a-zA-Z]', '', 'g')=regexp_replace(b.team_name,'[^a-zA-Z]', '', 'g')
    """

class AdjustedStat(object):

    def __init__(self, stat, dt):
        self.dt = dt
        self.date_seq = datetime.strftime(dt, '%Y%m%d')
        self.home_factor = 1.014
        self.stat = stat
        self.dayzero = self.start_date()
        self.daynum = (self.dt - self.dayzero).days

    def start_date(self):
        q = """SELECT dayzero FROM seasons WHERE season=%s""" % self.season()
        cur.execute(q)
        return cur.fetchone()[0]

    def date_string(self):
        return datetime.strftime(self.dt, '%Y-%m-%d')

    def season(self):
        if self.dt.month < 6:
            return self.dt.year
        else:
            return self.dt.year + 1

    def team_index(self):
        q = """SELECT team_id FROM teams"""
        cur.execute(q)
        results = cur.fetchall()

        self.team_indices = {results[k][0]: k for k in xrange(len(results))}
        self.nteams = len(self.team_indices)

    def preseason_rank(self, team_indices):
        preseason_o = np.zeros((self.nteams, 1))
        preseason_d = np.zeros((self.nteams, 1))

        self.query.kenpom(year)
        self.query.execute()
        for team in self.query.results:
            adjo = team[0]/float(100)
            adjd = team[1]/float(100)
            teamid = team[2]

            # this needs to be fixed.
            # I don't have preseason numbers for these adjusted stats
            if self.stat == 'trt':
                adjo = 0.2
                adjd = 0.2
            elif self.stat == 'efg':
                adjo = 0.5
                adjd = 0.5
            elif self.stat == 'ftr':
                adjo = 0.5
                adjd = 0.5
            preseason_d[self.team_indices[teamid]] = adjd
            preseason_o[self.team_indices[teamid]] = adjo

        return preseason_o, preseason_d

    def calc_stat(self, detailed):
        detailed['wposs'] = 0.96*(detailed.wfga - detailed.wor + 0.475*detailed.wfta)
        detailed['lposs'] = 0.96*(detailed.lfga - detailed.lor + 0.475*detailed.lfta)

        detailed['wppp'] = detailed.wscore / detailed.wposs
        detailed['lppp'] = detailed.lscore / detailed.lposs

        return detailed

    def filter_dataframe(self):
        cols = {'wloc': 0, 'wteam': 1, 'lteam': 2,
                'wposs': 3, 'lposs': 4, 'wppp': 5, 'lppp': 6}
        q = """ SELECT
                    wloc,
                    wteam,
                    lteam,
                    0.96*(wfga - wor + 0.475*wfta) AS wposs,
                    0.96*(lfga - lor + 0.475*lfta) AS lposs,
                    wscore / (0.96*(wfga - wor + 0.475*wfta)) AS wppp,
                    lscore / (0.96*(lfga - lor + 0.475*lfta)) AS lppp
                FROM reg_detailed 
                WHERE daynum + (SELECT dayzero FROM seasons WHERE season=%s) < '%s'
                AND season=%s
            """ % (self.season(), self.date_string(), self.season())
        cur.execute(q)
        return cur.fetchall(), cols

    def initialize(self):
        self.team_index()
        # detailed = pd.read_sql("""SELECT * FROM reg_detailed""", conn)
        stats, cols = self.filter_dataframe()
        # stats = self.calc_stat(detailed)

        raw_omat = np.empty((40, self.nteams))
        raw_dmat = np.empty((40, self.nteams))
        ind_mat = np.empty((40, self.nteams))
        loc_mat = np.empty((40, self.nteams))

        raw_omat.fill(np.nan)
        raw_dmat.fill(np.nan)
        ind_mat.fill(np.nan)
        loc_mat.fill(np.nan)
        r, c = raw_omat.shape

        for idx, game in enumerate(stats):
            stat = game[cols['wppp']]
            opp_stat = game[cols['lppp']]
            team = game[cols['wteam']]
            opp = game[cols['lteam']]

            if game[cols['wloc']] == 'H':
                loc_factor = self.home_factor
            elif game[cols['wloc']] == 'A':
                loc_factor = 1 / self.home_factor
            else:
                loc_factor = 1
            
            team_idx = self.team_indices[team]
            opp_idx = self.team_indices[opp]
            last_entry = raw_omat[r - 1][team_idx]

            non_nan_o = np.count_nonzero(~np.isnan(raw_omat[:, team_idx]))
            non_nan_d = np.count_nonzero(~np.isnan(raw_dmat[:, team_idx]))
            raw_omat[non_nan_o][team_idx] = stat
            raw_dmat[non_nan_d][team_idx] = opp_stat
            ind_mat[non_nan_o][team_idx] = opp_idx
            loc_mat[non_nan_o][team_idx] = loc_factor

            stat = game[cols['lppp']]
            opp_stat = game[cols['wppp']]
            team = game[cols['lteam']]
            opp = game[cols['wteam']]

            loc_factor = 1 / loc_factor

            team_idx = self.team_indices[team]
            opp_idx = self.team_indices[opp]
            last_entry = raw_omat[r - 1][team_idx]

            non_nan_o = np.count_nonzero(~np.isnan(raw_omat[:, team_idx]))
            non_nan_d = np.count_nonzero(~np.isnan(raw_dmat[:, team_idx]))
            raw_omat[non_nan_o][team_idx] = stat
            raw_dmat[non_nan_d][team_idx] = opp_stat
            ind_mat[non_nan_o][team_idx] = opp_idx
            loc_mat[non_nan_o][team_idx] = loc_factor

        return raw_omat, raw_dmat, ind_mat, loc_mat

    def weight_matrix(self, raw_omat, wtype=''):
        if wtype == 'linear':
            weights = np.zeros(raw_omat.shape)
            for c in xrange(raw_omat.shape[1]):
                col = raw_omat[:,c]
                n = np.sum(~np.isnan(col))
                if n == 0:
                    continue
                w = np.array(xrange(1, n+1))
                w = w * (1 / float(w.sum()))
                weights[:n,c] = w
            return weights
        else:
            game_counts = np.sum(~np.isnan(raw_omat), axis=0)
            weights = np.ones(raw_omat.shape)
            return weights / game_counts.astype(float)

    def rank(self):
        # create adjd matrix (will have nans) from adjd vec
        raw_omat, raw_dmat, ind_mat, loc_mat = self.initialize()
        adj_d = np.nanmean(raw_dmat, axis=0)
        adj_o = np.nanmean(raw_omat, axis=0)
        avg_o = np.nanmean(raw_omat)
        avg_d = np.nanmean(raw_dmat)

        # need weight matrix
        weights = self.weight_matrix(raw_omat, wtype='linear')

        for i in xrange(20):
            adj_dprev = adj_d*1
            adj_oprev = adj_o*1

            new_omat = raw_omat / adj_d[np.nan_to_num(ind_mat).astype(int)] * loc_mat * weights * avg_o
            new_dmat = raw_dmat / adj_o[np.nan_to_num(ind_mat).astype(int)] * (1 / loc_mat) * weights * avg_d

            # print new_omat

            adj_o = np.sum(np.nan_to_num(new_omat), axis=0)
            adj_d = np.sum(np.nan_to_num(new_dmat), axis=0)
            r_off = np.linalg.norm(np.nan_to_num(adj_oprev - adj_o))
            r_def = np.linalg.norm(np.nan_to_num(adj_dprev - adj_d))
            # print adj_oprev - adj_omat
            # break
            # print r_off
            # print adj_d[:10]

        return adj_o, adj_d

    def print_ranks(self, ranks, n=10, reverse=True):
        teams = pd.read_sql("""SELECT * FROM teams""", conn)
        rank_list = []
        for idx, row in teams.iterrows():
            rank = ranks[self.team_indices[row.team_id]]
            if rank == 0:
                continue
            rank_list.append((row.team_name, rank))

        print sorted(rank_list, key=lambda x: x[1], reverse=reverse)[:n]

    def store_ranks(self, ortg, drtg):
        cols = {'season': 0, 'daynum': 1, 'wteam': 2, 'lteam': 3,
                'w%s' % self.stat: 4, 'l%s' % self.stat: 5,
                'wd%s' % self.stat: 6, 'ld%s' % self.stat: 7}
        q = """ SELECT 
                    season,
                    daynum,
                    wteam,
                    lteam,
                    w{stat},
                    l{stat},
                    wd{stat},
                    ld{stat}
                FROM reg_advanced
                WHERE daynum + (SELECT dayzero FROM seasons WHERE season={season}) = '{dt}'
                AND season={season}
            """.format(stat=self.stat, season=self.season(), dt=self.date_string())
        cur.execute(q)
        vals = []
        for game in cur.fetchall():
            wteam = game[cols['wteam']]
            lteam = game[cols['lteam']]
            season = game[cols['season']]
            daynum = game[cols['daynum']]
            wrtg = ortg[self.team_indices[int(wteam)]]
            lrtg = ortg[self.team_indices[int(lteam)]]
            wdrtg = drtg[self.team_indices[int(wteam)]]
            ldrtg = drtg[self.team_indices[int(lteam)]]
            vals.append((wteam, lteam, season, daynum, wrtg, lrtg, wdrtg, ldrtg))
        # print vals[:40]
        q = """ UPDATE reg_advanced AS t SET
                w{stat}=c.w{stat},
                l{stat}=c.l{stat},
                wd{stat}=c.wd{stat},
                ld{stat}=c.ld{stat},
                wteam=c.wteam,
                lteam=c.lteam,
                season=c.season,
                daynum=c.daynum
                FROM (values {vals})
                AS c(wteam, lteam, season, daynum, w{stat}, l{stat}, wd{stat}, ld{stat})
                WHERE c.wteam=t.wteam
                AND c.lteam=t.lteam
                AND c.season=t.season
                AND c.daynum=t.daynum;
            """.format(vals=','.join(['%s' % str(v) for v in vals]),
                       stat=self.stat)
        cur.execute(q)
            # q = """ UPDATE reg_advanced
            #         SET w{stat}={wstat},
            #             l{stat}={lstat},
            #             wd{stat}={wdstat},
            #             ld{stat}={ldstat}
            #         WHERE season={season}
            #         AND daynum={daynum}
            #         AND wteam={wteam}
            #         AND lteam={lteam}
            #     """.format(season=self.season(),
            #                stat=self.stat,
            #                wstat=wrtg,
            #                lstat=lrtg,
            #                wdstat=wdrtg,
            #                ldstat=ldrtg,
            #                daynum=self.daynum,
            #                wteam=wteam,
            #                lteam=lteam)
            # cur.execute(q)
        conn.commit()

if __name__ == '__main__':
    # vals = [(1272,8,9),(1266,3,7)]
    # q = """ UPDATE reg_advanced AS t SET
    #         wppp=c.wppp,
    #         lppp=c.lppp,
    #         wdppp=c.wdppp,
    #         ldppp=c.ldppp,
    #         wteam=c.wteam
    #         FROM (values {vals})
    #         AS c(wteam, wppp, lppp, wdppp, ldppp)
    #         WHERE c.wteam=t.wteam;
    #     """.format(vals=','.join(['%s' % str(v) for v in vals]))
    # print q
    # cur.execute(q)
    # conn.commit()

    # return None

    start_date = datetime(2013, 2, 21).date()
    end_date = datetime(2013, 2, 28).date()
    day_count = (end_date - start_date).days + 1

    for single_date in (start_date + timedelta(n) for n in xrange(day_count)):
        a = AdjustedStat('ppp', single_date)
        adj_o, adj_d = a.rank()
        print 'storing.....'
        a.store_ranks(adj_o, adj_d)
        print single_date, a.daynum
