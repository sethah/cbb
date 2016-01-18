import pandas as pd
import numpy as np
import DB

def unstack_games(games):
    cols_to_check = ['hteam_id', 'ateam_id', 'home_score', 'away_score']
    for col in cols_to_check:
        assert col in games.columns
    games1 = games.rename(columns={'hteam_id': 'ateam_id', 'ateam_id': 'hteam_id'})
    df = pd.concat([games, games1], axis=0)
    df.rename(columns={'hteam_id': 'team', 'ateam_id': 'opp', 'home_score': 'score',
                       'away_score': 'opp_score'}, inplace=True)
    df['outcome'] = df.apply(lambda row: row.score > row.opp_score, axis=1)
    df.drop('home_outcome', axis=1, inplace=True)
    return df

if __name__ == "__main__":
    games = pd.read_sql("SELECT * from games_test", DB.conn)
    def season(dt):
        return dt.year if dt.month < 6 else dt.year + 1
    games['season'] = games.dt.map(season)
    games = games[games.season == 2013]
    df = unstack_games(games)


