import pandas as pd
import psycopg2

def create_compact(cur):
    q = """ CREATE TABLE reg_compact
            (
            season int    NOT NULL,
            daynum int   NOT NULL,
            wteam int   NOT NULL,
            wscore int   NOT NULL,
            lteam int   NOT NULL,
            lscore int   NOT NULL,
            wloc text   NOT NULL,
            numot int    NOT NULL
            )
        """
    cur.execute(q)
    q = """ \COPY reg_compact
            FROM 'regular_season_compact_results.csv'
            DELIMITER ','
            CSV HEADER;
        """
    cur.execute(q)


def create_detailed(cur):
    q = """ CREATE TABLE reg_detailed
            (
            season int    NOT NULL,
            daynum int   NOT NULL,
            wteam int   NOT NULL,
            wscore int   NOT NULL,
            lteam int   NOT NULL,
            lscore int   NOT NULL,
            wloc text   NOT NULL,
            numot int    NOT NULL,
            wfgm int    NOT NULL,
            wfga int    NOT NULL,
            wfgm3 int    NOT NULL,
            wfga3 int    NOT NULL,
            wftm int    NOT NULL,
            wfta int    NOT NULL,
            wor int    NOT NULL,
            wdr int    NOT NULL,
            wast int    NOT NULL,
            wto int    NOT NULL,
            wstl int    NOT NULL,
            wblk int    NOT NULL,
            wpf int    NOT NULL,
            lfgm int    NOT NULL,
            lfga int    NOT NULL,
            lfgm3 int    NOT NULL,
            lfga3 int    NOT NULL,
            lftm int    NOT NULL,
            lfta int    NOT NULL,
            lor int    NOT NULL,
            ldr int    NOT NULL,
            last int    NOT NULL,
            lto int    NOT NULL,
            lstl int    NOT NULL,
            lblk int    NOT NULL,
            lpf int    NOT NULL
            )
        """
    cur.execute(q)

def create_teams(cur):
    q = """ CREATE TABLE teams
            (
            team_id int PRIMARY KEY NOT NULL,
            team_name text NOT NULL
            )
        """
    cur.execute(q)


def create_seasons(cur):
    q = """ CREATE TABLE seasons
            (
            season int PRIMARY KEY NOT NULL,
            dayzero DATE   NOT NULL,
            regionW text    NOT NULL,
            regionX text    NOT NULL,
            regionY text    NOT NULL,
            regionZ text    NOT NULL
            )
        """
    cur.execute(q)

def create_seeds(cur):
    q = """ CREATE TABLE seeds
            (
            season int REFERENCES seasons(season) NOT NULL,
            seed text   NOT NULL,
            team int REFERENCES teams(team_id) NOT NULL
            )
        """
    cur.execute(q)

if __name__ == '__main__':
    conn = psycopg2.connect(database="cbb", user="seth", password="abc123",
                            host="localhost", port="5432")
    cur = conn.cursor()
    # create_detailed(cur)
    # create_teams(cur)
    # create_seasons(cur)
    # create_seeds(cur)
    conn.commit()
    conn.close()