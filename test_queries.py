q = """ WITH streak AS
        (SELECT 
            season,
            daynum, 
            team,
            opp_team,
            score,
            opp_score,
            outcome,
            COALESCE(sum(cast(outcome as int)) over(partition by (team, season) order by daynum rows between 5 preceding and 1 preceding) / CAST(5.0 as REAL), -1) as streak5
        FROM games_unstacked
        ORDER BY daynum)
        
        UPDATE games_unstacked g
        SET streak5=s.streak5
        FROM streak s
        WHERE g.season=s.season
        AND g.daynum=s.daynum
        AND g.team=s.team;
    """
q = """ SELECT
            season,
            daynum,
            wteam as team,
            lteam as opp_team,
            wscore as score,
            lscore as opp_score,
            (wscore > lscore) as outcome
        FROM reg_compact;
    """
q = """ CREATE TABLE games_unstacked AS
        WITH a AS
        (SELECT
            season,
            daynum,
            lteam as team,
            wteam as opp_team,
            lscore as score,
            wscore as opp_score,
            (lscore > wscore) as outcome
        FROM reg_compact),

        b AS
        (SELECT
            season,
            daynum,
            wteam as team,
            lteam as opp_team,
            wscore as score,
            lscore as opp_score,
            (wscore > lscore) as outcome
        FROM reg_compact)
        
        SELECT * from a UNION select * from b;
    """

q = """ WITH streak AS
        (SELECT team, season, daynum, streak5
        FROM games_unstacked)

        UPDATE reg_compact r
        SET wstreak5=g.streak5
        FROM games_unstacked g
        WHERE r.season=g.season
        AND r.daynum=g.daynum
        AND r.wteam=g.team;
    """

q = """ WITH streak AS
        (SELECT team, season, daynum, streak5
        FROM games_unstacked)

        UPDATE reg_compact r
        SET lstreak5=g.streak5
        FROM games_unstacked g
        WHERE r.season=g.season
        AND r.daynum=g.daynum
        AND r.lteam=g.team;
    """
q = """ WITH sub AS (SELECT * FROM tmp)
        
        UPDATE reg_compact r
        SET dt=a.dt
        FROM sub a
        WHERE a.season=r.season
        AND a.daynum=r.daynum
        AND r.wteam=a.wteam
        AND r.lteam=a.lteam
    """


q = """ CREATE TABLE kp_detailed AS
        WITH sub AS
        (SELECT a.*, b.team_id as team1_id, c.team_id as team2_id
        FROM kp a
        JOIN teams b
        ON a.team1=b.kenpom
        JOIN teams c
        ON a.team2=c.kenpom)
        
        SELECT a.*, b.loc, b.wloc,
            CASE WHEN b.wloc='H' then wteam else lteam end AS hteam,
            CASE WHEN b.wloc='H' then lteam else wteam end as ateam,
            CASE WHEN wloc='N' then 1 else 0 end AS neutral
        FROM sub a
        JOIN reg_compact b
        ON a.dt=b.dt
        AND (a.team1_id=b.wteam OR a.team1_id=b.lteam)
    """
q = """ CREATE TABLE tmp AS
        (SELECT a.season, a.daynum, b.dayzero, a.wteam, a.lteam,
               (b.dayzero + INTERVAL '1 day'*a.daynum)::date as dt
        FROM reg_compact a
        JOIN seasons b
        ON a.season=b.season)
        
        UPDATE reg_compact r
        SET dt=(b.dayzero + INTERVAL '1 day'*a.daynum)::date
        FROM reg_compact a
        JOIN seasons b
        ON a.season=b.season 
    """
q = """ UPDATE reg_compact r
        SET dt=t.dt
        FROM tmp t
        WHERE t.season=r.season
        AND """
q = """ CREATE TABLE cities AS
        WITH venue1 AS
        (SELECT team1, city, count(*)
        FROM kp
        GROUP BY team1, city),

        venue2 AS
        (SELECT team2, city, count(*)
        FROM kp
        GROUP BY team2, city),
        
        cities AS 
        (SELECT a.*, a.count + b.count AS total
        FROM venue1 a
        JOIN venue2 b
        ON a.team1=b.team2
        AND a.city=b.city),
        
        max_city AS
        (SELECT a.team1, max(a.total) as mx
        FROM cities a
        GROUP BY a.team1)
        
        SELECT a.team1, a.city, a.total
        FROM cities a
        JOIN max_city b
        ON a.team1=b.team1
        AND b.mx=a.total
    """
q = """ WITH sub AS (SELECT * FROM cities)
        
        UPDATE teams t
        SET city=a.city
        FROM sub a
        WHERE t.kenpom=replace(a.team1, ';', '')
    """
q = """ SELECT *
        FROM cities a
        LEFT OUTER JOIN teams b
        ON replace(a.team1, ';', '')=b.kenpom"""
q = """ INSERT INTO teams VALUES (1190, 'ETSU',198, 'east-tennesee-state', 'East Tenn. St.', 'East Tenn St', '2193', 'ETNST', 'e-tennessee-state', 'East Tennessee St.')"""
q = """ INSERT INTO teams VALUES(1322, 'Northwestern LA',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Northwestern St.','Slnd','Natchitoches')"""
q = """ INSERT INTO teams VALUES(1300, 'NC Central',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'North Carolina Central','MEAC','Durham')"""

q = """ SELECT a.team1,
               a.team2,
               a.city,
               b.city as city1,
               c.city as city2,
               CASE
                WHEN a.city=b.city THEN team1
                WHEN a.city=b.city THEN team
        from kp a
        join teams b
        on a.team1=b.kenpom
        join teams c
        on a.team2=c.kenpom;"""

q = """
        WITH venue1 AS
        (SELECT team1, venue, count(*)
        FROM kp
        GROUP BY team1, venue),

        venue2 AS
        (SELECT team2, venue, count(*)
        FROM kp
        GROUP BY team2, venue)
        
        SELECT a.*, a.count + b.count AS total
        FROM venue1 a
        JOIN venue2 b
        ON a.team1=b.team2
        AND a.venue=b.venue"""

