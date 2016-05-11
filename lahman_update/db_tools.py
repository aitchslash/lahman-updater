"""Utilities for modifying the database."""

import pymysql


def get_db_login(path='../data/db_details.txt'):
    """Get login details for database from file."""
    with open(path, 'r') as f:
        lines = [line.strip() for line in f]
        login_dict = {i.split(":")[0].strip(): i.split(":")[1].strip()
                      for i in lines}
    return login_dict

db = get_db_login()
lahmandb = db['db']
host = db['host']
username = db['username']
password = db['password']


def get_columns(table):
    """Get column names from Lahman table."""
    mydb = pymysql.connect(host, username, password, lahmandb)
    cursor = mydb.cursor()
    statement = "SHOW columns FROM %s" % (table)
    print statement  # nb, test line
    cursor.execute(statement)
    query_results = cursor.fetchall()
    cursor.close()
    columns = [elem[0] for elem in query_results]
    return columns


def fix_chicago_team_data():
    """Fix errors in Chicago team data.

    The 2014 db may have mismatches between the Cubs and the White Sox.
    This checks for the mismatches in the teams table for 2013 and 2014 and corrects it if present.
    """
    mydb = pymysql.connect(host, username, password, lahmandb)
    cursor = mydb.cursor()
    statement = """SELECT franchID, teamID, yearID, name, park, teamIDBR FROM teams
                   WHERE yearID IN (2013, 2014) AND
                   teamID LIKE 'CH%'"""
    cursor.execute(statement)
    results = cursor.fetchall()
    # check for an error
    checks = [x[0] == x[-1] for x in results]
    # either all checks should be fine or should be broken
    assert checks[0] == checks[1] == checks[2] == checks[3]
    match_ups = []
    if checks[0] is False:
        # print "let us fix stuff."
        # the problems are the last 8 columns in teams
        problem_columns = get_columns('teams')[-8:]
        prob_statement = """SELECT yearID, teamID, """
        for field in problem_columns:
            prob_statement += field + ", "
        prob_statement = prob_statement[:-2]
        prob_statement += """ FROM teams WHERE yearID IN (2013, 2014) AND teamID LIKE 'CH%'
                             ORDER BY yearID DESC"""
        # print prob_statement
        cursor.execute(prob_statement)
        bad_data = cursor.fetchall()
        # hmm
        start_end = [[0, 1], [1, 0], [2, 3], [3, 2]]
        for pair in start_end:
            # print pair[0]
            # print bad_data[pair[0]]

            where_equals = list(bad_data[pair[0]][:2])
            # print start
            set_values = list(bad_data[pair[1]][-8:])
            match_ups.append((where_equals, set_values))
        for match in match_ups:
            update_string = """UPDATE teams SET """
            for count, value in enumerate(match[1]):
                update_string += problem_columns[count] + "="
                if isinstance(value, str):
                    update_string += "'" + value + "', "
                else:
                    update_string += str(value) + ", "
            update_string = update_string[:-2]
            update_string += " WHERE yearID = {} and teamID = '{}'".format(match[0][0], match[0][1])
            cursor.execute(update_string)

    mydb.commit()
    cursor.close()
    return match_ups


def add_pitching_columns():
    """Run once.  Add extra stat columns to pitching."""
    mydb = pymysql.connect(host, username, password, lahmandb)
    cursor = mydb.cursor()
    statement = """ALTER TABLE pitching
                   ADD COLUMN ROE TINYINT(2) DEFAULT NULL,
                   ADD COLUMN BAbip DOUBLE DEFAULT NULL AFTER BAopp,
                   ADD COLUMN OPS DOUBLE DEFAULT NULL AFTER BAopp,
                   ADD COLUMN SLG DOUBLE DEFAULT NULL AFTER BAopp,
                   ADD COLUMN OBP DOUBLE DEFAULT NULL AFTER BAopp,
                   ADD COLUMN WHIP DOUBLE DEFAULT NULL AFTER BAopp,
                   ADD COLUMN ERAplus SMALLINT DEFAULT NULL AFTER BAopp,
                   ADD COLUMN FIP DOUBLE DEFAULT NULL AFTER BAopp,
                   ADD COLUMN PA SMALLINT DEFAULT NULL AFTER BFP,
                   ADD COLUMN SOperW DOUBLE DEFAULT NULL AFTER SO"""
    cursor.execute(statement)
    mydb.commit()
    cursor.close()


def add_mlbamid_master():
    """Add column for mlb adv media ID to master."""
    mydb = pymysql.connect(host, username, password, lahmandb)
    cursor = mydb.cursor()
    statement = """ALTER TABLE master
                   ADD COLUMN mlbamID INT(11) DEFAULT NULL"""
    cursor.execute(statement)
    mydb.commit()
    cursor.close()
