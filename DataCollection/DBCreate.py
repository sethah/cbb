from DB import DB

TABLES = {"games": "games",
          "box": "box_stats",
          "pbp": "pbp_stats",
          "raw_pbp": "raw_pbp",
          "division_one": "division_one",
          "teams": "teams",
          "kenpom_ranks": "kenpom_ranks"}

def get_table_name(table):
    return TABLES.get(table)

def create_games():
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
        """.format(games=get_table_name("games"))
    return q

def create_ncaa_box():
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
        """.format(box=get_table_name("box"),
                   games=get_table_name("games"))
    return q

def create_pbp():
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
        """.format(pbp=get_table_name("pbp"),
                   games=get_table_name("games"),
                   raw_pbp=get_table_name("raw_pbp"))

    return q

def create_raw_pbp():
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
    """.format(raw_pbp=get_table_name("raw_pbp"),
               games=get_table_name("games"))

    return q

def create_division_one():
    q = """ CREATE TABLE {division_one}
            (
            id SERIAL PRIMARY KEY,
            ncaaid INT NOT NULL,
            year INT NOT NULL,
            UNIQUE(ncaaid, year)
            )
        """.format(division_one=TABLES.get("division_one"),
                   teams=TABLES.get("teams"))
    return q

def create_kenpom_ranks():
    q = """ CREATE TABLE {kenpom_ranks}
            (
            id SERIAL PRIMARY KEY,
            rank INT NOT NULL,
            team TEXT NOT NULL,
            conf TEXT NOT NULL,
            wins INT NOT NULL,
            losses INT NOT NULL,
            pyth REAL NOT NULL,
            adjo REAL NOT NULL,
            adjd REAL NOT NULL,
            adjt REAL NOT NULL,
            luck TEXT NOT NULL,
            sos_pyth REAL NOT NULL,
            sos_opp_o REAL NOT NULL,
            sos_opp_d REAL NOT NULL,
            ncsos REAL NOT NULL,
            year INT NOT NULL,
            UNIQUE(team, year)
            )
        """.format(kenpom_ranks=TABLES.get("kenpom_ranks"))
    return q

def add_table(qfunc):
    q = qfunc()
    cur = DB.conn.cursor()
    print q
    try:
        cur.execute(q)
        DB.conn.commit()
    except:
        DB.conn.rollback()
        print "failed to add table"
