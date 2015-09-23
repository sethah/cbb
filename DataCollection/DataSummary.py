import pandas as pd

from DataCollection import DB


class DataSummarizer(object):

    @staticmethod
    def season(dt):
        return dt.year if dt.month < 6 else dt.year + 1

    def __init__(self):
        self.conn = DB.conn
        self.cur = self.conn.cursor()

    def games_in_db(self, total_games_gb, table_name):
        """
        INPUT: DataSummarizer, GROUPBY, STRING
        OUTPUT: DATAFRAME, Int, Int, Float

        Summarize proportion of games in the table `table_name`
        """
        q = """
            SELECT b.dt
            FROM (SELECT DISTINCT(game_id) AS gid FROM {table_name}) a
            JOIN games_ncaa b
            ON a.gid=b.game_id
            """.format(table_name=table_name)
        df = pd.read_sql(q, self.conn)
        df['season'] = df.dt.map(DataSummarizer.season)
        gb_pbp = df.groupby('season').count()
        merged = pd.merge(total_games_gb, gb_pbp, left_index=True, right_index=True)
        merged['pct'] = merged['dt_y'] / merged['dt_x'].astype(float)
        merged.columns = ['total', 'in_db', 'pct']
        games_in_db = merged.in_db.sum()
        total_games = merged.total.sum()
        pct_total = float(games_in_db) / total_games
        return merged, games_in_db, total_games, pct_total

    def print_game_summary(self, table_name, merged, cnt, total, pct):
        print "Table: %s" % table_name
        print "-"*20
        print merged
        print "\n%s of %s games (%0.2f%%)" % (cnt, total, pct*100)
        print "-"*20

    def game_summary(self):
        q = """SELECT dt FROM games_ncaa"""
        df = pd.read_sql(q, self.conn)
        df['season'] = df.dt.map(DataSummarizer.season)
        gb = df.groupby('season').count()
        print "Table: games_ncaa"
        print "-"*20
        print gb
        print "-"*20

        merged, games_in_db, total_games, pct_total = self.games_in_db(gb, 'pbp')
        print self.print_game_summary('pbp', merged, games_in_db, total_games, pct_total)

        merged, games_in_db, total_games, pct_total = self.games_in_db(gb, 'raw_pbp')
        print self.print_game_summary('raw_pbp', merged, games_in_db, total_games, pct_total)

        merged, games_in_db, total_games, pct_total = self.games_in_db(gb, 'ncaa_box')
        print self.print_game_summary('ncaa_box', merged, games_in_db, total_games, pct_total)

if __name__ == "__main__":
    summarizer = DataSummarizer()
    summarizer.game_summary()
