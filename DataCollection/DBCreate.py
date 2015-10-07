import psycopg2

class DBCreate(object):

    def __init__(self):
        self._tables = {"games": "games",
                        "box": "box_stats",
                        "pbp": "pbp_stats",
                        "raw_pbp": "raw_pbp"}

    def set_table_name(self, table, table_name):
        assert table in self._tables, \
            "Invalid table %s. Valid tables: %s" % (table, self._tables.keys())
        assert type(table_name) == str, "table_name must be a string"

        self._tables[table] = table_name

    def get_table_name(self, table):
        return self._tables.get(table)

    def create_games(self):
        q = """ CREATE TABLE {games}
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
            """.format(games=self.get_table_name("games"))
        return q

    def create_ncaa_box(self):
        q = """ CREATE TABLE {box}
                (
                game_id INT REFERENCES {games}(game_id) NOT NULL,
                team TEXT NOT NULL,
                first_name TEXT NOT NULL,
                last_name TEXT,
                pos TEXT,
                min INT,
                fgm int,
                fga int,
                tpm int,
                tpa int,
                ftm int,
                fta int,
                pts int,
                oreb int,
                dreb int,
                reb int,
                ast int,
                turnover int,
                stl int,
                blk int,
                pf int
                )
            """.format(box=self.get_table_name("box"),
                       games=self.get_table_name("games"))
        return q

    def create_pbp(self):
        q = """ CREATE TABLE {table_name}
                (
                game_id INT REFERENCES {games}(game_id) NOT NULL,
                pbp_id INT REFERENCES {raw_pbp}(id) NOT NULL,
                team TEXT NOT NULL,
                teamid INT,
                time REAL,
                first_name TEXT NOT NULL,
                last_name TEXT,
                play TEXT,
                hscore INT,
                ascore INT,
                possession INT,
                poss_time_full INT NOT NULL,
                poss_time INT,
                home_fouls INT NOT NULL,
                away_fouls INT NOT NULL,
                second_chance INT,
                timeout_pts INT,
                turnover_pts INT,
                and_one INT,
                blocked BOOLEAN,
                stolen BOOLEAN,
                assisted BOOLEAN,
                assist_play TEXT,
                recipient TEXT,
                charge BOOLEAN,
                UNIQUE(pbp_id)
                )
            """.format(pbp=self.get_table_name("pbp"),
                       games=self.get_table_name("games"),
                       raw_pbp=self.get_table_name("raw_pbp"))

        return q

    def create_raw_pbp(self):
        q = """ CREATE TABLE {raw_pbp}
            (
            id SERIAL PRIMARY KEY,
            game_id INT REFERENCES {games}(game_id) NOT NULL,
            team TEXT NOT NULL,
            teamid INT,
            time REAL,
            first_name TEXT NOT NULL,
            last_name TEXT,
            play TEXT,
            hscore INT,
            ascore INT
            )
        """.format(raw_pbp=self.get_table_name("raw_pbp"),
                   games=self.get_table_name("games"))

        return q
