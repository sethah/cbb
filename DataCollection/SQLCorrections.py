"""
select count(distinct(game_id))
from ncaa_box
where first_name != 'Totals'
and (pts > 50 or turnover > 10 or ast > 15
     or reb > 25 or min > 60 or blk > 20
     or stl > 15 or pf > 5);
"""
"""
select *
from ncaa_box
where first_name != 'Totals'
and (pts > 50 or turnover > 10 or ast > 15
     or reb > 25 or min > 60 or blk > 20
     or stl > 15 or pf > 5);
"""
"""
select count(distinct(game_id))
from ncaa_box
where first_name != 'Totals'
and (pf > 5);
"""
def correction_query(col, thresh):
    for decade in [(10, 100), (100, 1000), (1000, 10000)]:
        q = """
                UPDATE ncaa_box
                SET {col} = {col} / {div}
                WHERE {col} > {thresh}
                AND first_name != 'Totals'
                AND {col} % {div} = 0
                AND ({col} > {lb} AND {col} < {ub});
            """.format(
            col=col, thresh=thresh, div=decade[0],
            lb=decade[0]-1, ub=decade[1]
        )
        print q

if __name__ == "__main__":
    correction_query('min', 60)
    """
    UPDATE ncaa_box
    SET pf = pf / 100
    WHERE pf % 100 = 0
    AND (pf > 999 AND pf < 10000);
    """
    """
    SELECT count(*)
    FROM ncaa_box
    WHERE pf % 10 = 0
    AND (pf > 9 AND pf < 1000)
    """