import psycopg2

class DBCreate(object):

    @staticmethod
    def create_games():
        q = """ CREATE TABLE games_test
                (
                dt DATE    NOT NULL,
                hteam_id INT,
                ateam_id INT,
                home_score INT,
                away_score INT,
                neutral BOOLEAN,
                neutral_site TEXT,
                home_outcome BOOLEAN,
                numot INT,
                game_id INT,
                opp_string TEXT,
                UNIQUE (game_id),
                UNIQUE (dt, hteam_id, ateam_id)
                )
            """