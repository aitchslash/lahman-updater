# /usr/bin/env python
"""
This reads the 2015 MLB hitting stats from a csv from bbref.

In order to extract the bbref ID's a copy of the html will
have to be scraped as well.

Notes:
1) Adding mlb advanced media ID as default.  Using INT(11)
2) Ensure that "Hide non-qualifiers for rate stats" is the same on all docs
:   if importing by hand.
3) Stint is not assured to be correct. Relies on bbref order
4) no recorded out leads to an infinite ERA - using 99.99
5) TeamID's for Chicago teams seem odd (Cubs = CHA, WS = CHN)
:   seems only to be for 2013 and 2014 (only looked at 2014db)
6) Setting new players last game to year-12-31 so they're easy to find
7) Rookie insert take a while on first run
8) Rookie birthState truncated to 2 chars
:   may want to expand column to accept more
:   or find converter
9) An open issue w/ Spynner throws an AttributeError, currently uncaught
:   poster on Stack Overflow got something similar to work w/ phantomJS
10) Adjusting wait_load in utils/scraper.py is a tradeoff: speed/reliability
11) Rookie missing from Chadwick (34 as of Apr 13/16)
:   SELECT * FROM master WHERE bbrefID IS NULL
12) Assumes you are using lahman 2014 version.  When tested on 2012 differnces
:   in structure and data types both threw errors.
"""

import sys
from utils.argparser import process_args, set_default_season
from utils.scraper import check_files, get_all_data
import csv
from bs4 import BeautifulSoup
import pymysql
import os
import urllib2
import time
import pprint  # nb, just for test prints

people_csv = 'data/data2015/people.csv'

cur_season = set_default_season()


def get_db_login(path='data/db_details.txt'):
    """Get login details for database from file."""
    with open(path, 'r') as f:
        lines = [line.strip() for line in f]
        login_dict = {i.split(":")[0].strip(): i.split(":")[1].strip()
                      for i in lines}
    return login_dict

# database login info  # may have to pass this through funcs

db = get_db_login()
lahmandb = db['db']
host = db['host']
username = db['username']
password = db['password']


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


def get_ids(page, fielding=False):
    """Turn html page into a BeautifulSoup."""
    """Return a dictionary mapping {name:bbref_id}."""
    with open(page, "r") as html:
        soup = BeautifulSoup(html, 'html.parser')

    name_bbref_dict = {}
    # hope = soup.find('div', {"id": "all_players_standard_pitching"})
    if fielding is False:
        all_players_div = soup.select('div[id^all_players_]')
    else:
        all_players_div = soup.find_all('table', id='players_standard_fielding')
    assert len(all_players_div) == 1  # should be one and only one
    tags = all_players_div[0].select('a[href^=/players/]')

    # old line, would work w/ 2016 if Missing name included in if cond.
    # tags = soup.select('a[href^=/players/]')
    for tag in tags:
        # format and deal w/ unicode
        name = (tag.parent.text)  # old working (pre-scraper) line
        name = name.encode('ascii', 'replace')  # .decode('ascii')
        name = name.replace('?', ' ')
        if name[-1].isalnum() is False:
            name = name[:-1]
        if name.find(' ') == -1:  # error checking
            print "Missing name for: ",
            pprint.pprint(name)
        addy = str(tag['href'])
        bbref_id = addy[addy.rfind('/') + 1: addy.rfind('.')]
        # assert name_bbref_dict.has_key(name) is False
        name_bbref_dict[name] = bbref_id

    return name_bbref_dict  # soup is only temporary


def fix_csv(func):
    """Decorate funcs using csv."""
    def inner(*args, **kwargs):
        """Clear csv of blank line(s)."""
        f = open(args[0], "r+")
        lines = f.readlines()
        f.seek(0)
        for line in lines:
            if line != "\n":
                f.write(line)
        f.truncate()
        f.close()
        return func(*args, **kwargs)
    return inner


@fix_csv
def make_people_dict(people_csv):
    """Return dictionary mapping bbref to master table data."""
    """Data from chadwick bureau."""
    people_dict = {}
    with open(people_csv, 'rb') as f:
        reader = csv.DictReader(f)
        hed = reader.fieldnames

        # rename keys to match lahman
        for i in range(0, len(hed)):
            if hed[i].find('key_') == 0:
                hed[i] = hed[i][4:] + "ID"
        for i in range(0, len(hed)):
            j = hed[i].find("_")
            if j != -1:
                hed[i] = hed[i][:j] + hed[i][j + 1].upper() + hed[i][j + 2:]

        for row in reader:
            # limit dictionary to those youger than 50
            if row['birthYear'] and int(row['birthYear']) > int(cur_season) - 50:
                bbref_id = row['bbrefID']
                people_dict[bbref_id] = row
    return people_dict


def fix_mismatches(stats_dict_maker):
    """Run make_bbrefid_stats_dict fixing mismatches."""
    """Change bbrefID to LahmanID if necessary."""
    # don't know if args/kwargs needed in def
    def inner(*args, **kwargs):
        """Run maker."""
        stats_dict = stats_dict_maker(*args, **kwargs)
        mydb = pymysql.connect(host, username, password, lahmandb)
        cursor = mydb.cursor()
        statement = '''SELECT playerID, bbrefID
                   FROM master
                   WHERE birthYear > %s AND
                   DATE(finalGame) > "%s-1-1" AND
                   playerID != bbrefID''' % (str(int(cur_season) - 50),
                                             str(int(cur_season) - 5))
        cursor.execute(statement)
        mismatches = cursor.fetchall()

        for mm in mismatches:
            if mm[1] in stats_dict.keys():
                stats_dict[mm[0]] = stats_dict[mm[1]]
                print "bbrefID changed to lahman: ",
                print stats_dict[mm[0]]['Name']
                del stats_dict[mm[1]]

        cursor.close()
        return stats_dict
    return inner


@fix_csv
@fix_mismatches
def make_bbrefid_stats_dict(bbref_csv, name_bbref_dict, table='batting'):
    """Make dictionary mapping bbrefID (as key) to extracted stats (values)."""
    """multiple teams stats are nested as stints"""

    stats_dict = {}
    non_match = []
    with open(bbref_csv, "rb") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        if table == 'P':
            # fix Putouts/Pickoffs for P fielding
            header[-1] = 'PK'
        # read the lines from the csv
        for row in reader:
            name = row['Name']
            if row['Tm'] != 'TOT':
                try:
                    if name[-1].isalnum() is False:
                        # remove trailing character from name
                        row['Name'] = row['Name'][:-1]
                    bbref_id = name_bbref_dict[row['Name']]
                    # check for existing entry
                    if bbref_id not in stats_dict.keys():
                        stats_dict[bbref_id] = row
                    # then entry exists check if stints haven't been used
                    elif len(stats_dict[bbref_id]) > 10:
                        first_stint = stats_dict[bbref_id]
                        second_stint = row
                        del stats_dict[bbref_id]
                        stats_dict[bbref_id] = {'stint1': first_stint,
                                                'stint2': second_stint}
                    # then stints have been used, add this one
                    else:
                        st_num = "stint" + str(len(stats_dict[bbref_id]) + 1)
                        stats_dict[bbref_id].update({st_num: row})

                except:
                    if row['Name'] != 'Name':
                        non_match.append(row['Name'])

    print "non-match: ",
    print non_match  # warning
    return stats_dict


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


def make_team_dict():
    """Return dictionary mapping team_ids, lahman to bbref."""
    mydb = pymysql.connect(host, username, password, lahmandb)
    cursor = mydb.cursor()
    statement = '''SELECT teamID, teamIDBR
                   FROM teams
                   WHERE yearID = 2014'''
    cursor.execute(statement)
    team_tuples = cursor.fetchall()
    # print len(team_tuples)
    cursor.close()
    team_dict = {team[1]: team[0] for team in team_tuples}
    return team_dict


def insert_year(year=cur_season, expanded=True, fielding=False):
    """Update lahmandb with current year stats."""
    """Checks data files in place.  Assumes no existing data."""
    bats_html = os.path.join('', 'data', 'data' + year, 'bats.shtml')
    assert os.path.isfile(bats_html)
    ids = get_ids(bats_html)
    arms_html = os.path.join('', 'data', 'data' + year, 'arms.shtml')
    assert os.path.isfile(arms_html)
    p_ids = get_ids(arms_html)
    bats_csv = os.path.join('', 'data', 'data' + year, 'bats.csv')
    assert os.path.isfile(bats_csv)
    batting_dict = make_bbrefid_stats_dict(bats_csv, ids, table='batting')
    # old lines commented out, might be useful if not adding expanded data
    arms_csv = os.path.join('', 'data', 'data' + year, 'arms.csv')
    assert os.path.isfile(arms_csv)
    pitching_dict = make_bbrefid_stats_dict(arms_csv, p_ids, table='pitching')
    # a, pitching_dict, c = expand_p_test()
    batting_cols = get_columns('batting')
    pitching_cols = get_columns('pitching')
    # if pitching cols need to be added i.e. != 40 or == 30
    if expanded is True:
        if len(pitching_cols) == 30:
            add_pitching_columns()
            pitching_cols = get_columns('pitching')
        pitching_dict = expand_pitch_stats(pitching_dict, year)
    # team_dict = make_team_dict()
    id_set = set()
    id_set.update(pitching_dict.keys())
    id_set.update(batting_dict.keys())
    rookie_set = set(find_rookies(id_set))
    populate_master(rookie_set, year, expanded=expanded)
    print "Gathering rookie data may take a while..."
    ins_table_data(batting_dict, batting_cols, year, table='batting')
    ins_table_data(pitching_dict, pitching_cols, year, table='pitching')
    if fielding is True:
        ins_fielding(year)
    # batting_dict, team_dict, batting_cols, pitching_dict, pitching_cols
    return


def main():
    """Testing cmd line getter."""
    options = process_args(sys.argv[1:])
    # toggle orthodox/expanded
    if options['orthodox'] is True:
        expanded = False
    else:
        expanded = True
    '''
    for option in options:
        print str(option) + ': ' + str(options[option])
    '''

    try:
        latest_year = find_latest_year()
    except:
        print "Error connecting to database."
        # check db path is file
        try:
            login_dict = get_db_login(options['dbloginfile'])
            if len(login_dict) != 4:
                print "Something is wrong with your login file."
                print "Consider running setup.py"
            else:
                print "Path to login file looks OK. Make sure db is running."
        except:
            print "Unable to extract login details."
            print "Please check your path."
        sys.exit()
    if not 1876 <= int(options['year']) <= int(cur_season):
        print "Baseball data only available from 1876 to " + cur_season
        sys.exit()

    if latest_year != int(options['year']) and options['ignore'] is False:
        print "Preventing overwrite of lahman original data."
        print "Run setup.py to get past years or use --ignore to force update"
        sys.exit()

    # if options['ignore'] is False:
    past_due, exists = check_files(year=options['year'],
                                   expiry=options['expiry'],
                                   fielding=options['fielding'],
                                   chadwick=options['chadwick'])
    if past_due is False and exists is True and options['ignore'] is False:
        print "Data is fresh. Use --ignore to force refresh or change --expiry"
        sys.exit()

    # if forced and this year, current year is expired, or data missing: get data
    if ((options['ignore'] and str(options['year']) == cur_season) or
       (past_due and str(options['year']) == cur_season) or not exists):
        print "Getting data from baseball-reference."
        print "Spynner windows may open."
        print "Ignore uncaught AttributeError"
        past_due, exists = get_all_data(year=options['year'], expiry=options['expiry'],
                                        fielding=options['fielding'], chadwick=options['chadwick'])
        # check data retrevial went OK.
        if past_due is True or exists is False:
            print "Error with data. Please refetch."
            sys.exit()

    # OK, data gotten if needed.
    # if current year, run insert
    if str(options['year']).strip() == str(cur_season).strip():
        print "Run insert here."
        tables = ['batting', 'pitching', 'fielding']
        if not options['fielding']:
            tables.pop()
        for table in tables:
            reset_table(table=table, year=cur_season)
        insert_year(year=str(options['year']), expanded=expanded,
                    fielding=options['fielding'])
    # if latest data is earlier than the year we're working with database is empty there.
    elif latest_year < int(options['year']):  # could insist that it's only one year more.
        insert_year(year=str(options['year']), expanded=expanded,
                    fielding=options['fielding'])
    # else year earlier and data in both files and database - update with expanded.
    else:
        print "Run update here"

        # else:
        #   ensure expanded is True
        #   check columns - run insert if not there
        #   run an update
    return latest_year


def ins_table_data(table_data, cols_array, year, table='batting'):
    """Insert table data for batting or pitching."""
    team_dict = make_team_dict()
    if table == 'batting':
        insert_player = insert_batter
    else:
        insert_player = insert_pitcher
    print 'setup run, opening db...'
    print 'inserting into ' + table + "..."
    mydb = pymysql.connect(host, username, password, lahmandb)
    cursor = mydb.cursor()
    for key in table_data.keys():
        statements = insert_player(key, table_data, team_dict, cols_array, year)
        for statement in statements:
            cursor.execute(statement)
    mydb.commit()
    cursor.close()
    return


# key will be bbref_dict.keys() i.e. bbref_id
def insert_batter(key, stats_dict, team_dict, fields_array, year):
    """Create insertion string(s) batting."""
    """Returns an array of sql commands to be executed."""
    stats = stats_dict[key]
    stints = []
    if len(stats) == 30:  # only one stint
        stats['stint'] = 1
        stints.append(stats)
    else:
        for stint_key in stats.keys():
            stats[stint_key]['stint'] = str(stint_key[-1])
            stints.append(stats[stint_key])

    insert_strings = []
    # empty_warning = []  # nb, likely served its purpose
    for stint in stints:
        statement_start = "INSERT INTO batting ("
        for field in fields_array:
            statement_start += field + ", "
        statement_start = statement_start[:-2] + ") VALUES ("
        ss = statement_start
        ss += "'" + key + "', "
        ss += year + ", "
        ss += str(stint['stint']) + ", "  # stint + ", "
        ss += "'" + team_dict[stint["Tm"]] + "', "
        ss += "'" + stint['Lg'] + "', "
        ss += stint['G'] + ', '
        ss += "NULL" + ', '
        stat_keys = ['AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'SB',
                     'CS', 'BB', 'SO', 'IBB', 'HBP', 'SH', 'SF', 'GDP']
        for sk in stat_keys:
            # this could be reworked w/ stint.get(sk, default = 0)
            if stint[sk]:
                ss += stint[sk] + ', '
            else:
                ss += '0, '
                # empty_warning.append(key)  # nb, this is likely done too.
        ss += "NULL)"
        insert_strings.append(ss)

    # if empty_warning:  # nb, can likely get rid of this
    #    pprint.pprint(empty_warning)
    return insert_strings


def ins_fielding(year):
    """Insert fielding stats into db."""
    positions = ['P', 'C', '1B', '2B', '3B', 'SS', 'RF', 'LF', 'CF']

    team_dict = make_team_dict()
    cols = get_columns('fielding')

    print 'setup run, opening db...'

    for pos in positions:
        csv_path, html_path = make_paths(pos)
        pos_data = get_ids(html_path, fielding=True)
        pos_dict = make_bbrefid_stats_dict(csv_path, pos_data, pos)
        print 'inserting into ' + "fielding " + pos + " ..."
        mydb = pymysql.connect(host, username, password, lahmandb)
        cursor = mydb.cursor()
        for key in pos_dict.keys():
            statements = insert_fielder(key, year, pos_dict, team_dict, cols, pos)
            for statement in statements:
                cursor.execute(statement)
        mydb.commit()
        print "data committed"
        cursor.close()
    return


def insert_fielder(key, year, stats_dict, team_dict, fields_array, position):
    """Make insert string for fielding."""
    stats = stats_dict[key]
    stints = []

    if len(stats) > 10:  # only one stint
        stats['stint'] = 1
        stints.append(stats)
    else:  # unpack stints into the array
        for stint_key in stats.keys():
            stats[stint_key]['stint'] = str(stint_key[-1])
            stints.append(stats[stint_key])

    # move InnOuts to end of array
    fields_array.append(fields_array.pop(fields_array.index('InnOuts')))

    insert_strings = []
    for stint in stints:
        statement_start = "INSERT INTO fielding ("
        for field in fields_array:
            statement_start += field + ", "
        statement_start = statement_start[:-2] + ") VALUES ("
        ss = statement_start
        ss += "'" + key + "', "
        ss += year + ", "
        ss += str(stint['stint']) + ", "  # stint + ", "
        ss += "'" + team_dict[stint["Tm"]] + "', "
        ss += "'" + stint['Lg'] + "', "
        ss += "'" + position.upper() + "', "
        stat_keys = ['G', 'GS', 'PO', 'A', 'E', 'DP',
                     'PB', 'WP', 'SB', 'CS']
        for sk in stat_keys:
            try:
                if stint[sk]:
                    ss += stint[sk] + ', '
                else:
                    ss += '0, '
            except:  # missing key, e.g. PB for RFer
                ss += 'NULL, '
        # ZR
        ss += 'NULL, '
        # InnOuts
        last_digit = str(float(stint['Inn']))[-1]
        ipouts = int(float(stint['Inn'])) * 3 + int(last_digit)
        ss += str(ipouts) + ')'
        insert_strings.append(ss)
    return insert_strings


def insert_pitcher(key, stats_dict, team_dict, fields_array, year):
    """Create insertion string(s)."""
    """Returns an array of sql commands to be executed."""
    stats = stats_dict[key]
    stints = []
    if len(stats) > 10:
        stats['stint'] = 1
        stints.append(stats)
    else:
        for stint_key in stats.keys():
            stats[stint_key]['stint'] = str(stint_key[-1])
            stints.append(stats[stint_key])

    # move IPouts to end of array
    fields_array.append(fields_array.pop(fields_array.index('IPouts')))

    insert_strings = []
    empty_warning = set()  # nb, likely served its purpose
    for stint in stints:
        statement_start = "INSERT INTO pitching ("
        for field in fields_array:
            statement_start += field + ", "
        statement_start = statement_start[:-2] + ") VALUES ("
        ss = statement_start
        ss += "'" + key + "', "
        ss += year + ", "
        ss += str(stint['stint']) + ", "
        ss += "'" + team_dict[stint["Tm"]] + "', "
        ss += "'" + stint['Lg'] + "', "

        stat_keys = fields_array[5:-1]
        for sk in stat_keys:
            if stint[sk]:
                if stint[sk] != 'inf':  # ERA = infinity
                    ss += stint[sk] + ', '
                else:
                    ss += '99.99, '  # arbitrarily set ERA to 99.99
            else:
                ss += '0, '
                empty_warning.add(key)  # nb, this is likely done too.

        # lines for IPouts
        last_digit = str(float(stint['IP']))[-1]
        ipouts = int(float(stint['IP'])) * 3 + int(last_digit)
        ss += str(ipouts) + ')'
        insert_strings.append(ss)
        '''
    if empty_warning:  # nb, can likely get rid of this
        pprint.pprint(empty_warning)'''
    return insert_strings


def expand_pitch_stats(pitching_dict, year=cur_season):
    """Add new stats to pitching_dict."""
    arms_exp_html = os.path.join('', 'data', 'data' + year, 'arms_extra.shtml')
    assert os.path.isfile(arms_exp_html)
    ids = get_ids(arms_exp_html)
    arms_extra_csv = os.path.join('', 'data', 'data' + year, 'arms_extra.csv')
    assert os.path.isfile(arms_extra_csv)
    sd = make_bbrefid_stats_dict(arms_extra_csv, ids)
    # make sure the two pages match up
    assert sd.keys() == pitching_dict.keys()

    for p_id in sd.keys():
        if len(sd[p_id].keys()) > 10:  # only one stint
            sd[p_id].update(pitching_dict[p_id])
            sd[p_id]['BAOpp'] = sd[p_id].pop('BA')
            sd[p_id]['GIDP'] = sd[p_id].pop('GDP')
            sd[p_id]['BFP'] = sd[p_id].pop('BF')
            sd[p_id]['ERAplus'] = sd[p_id].pop('ERA+')
            sd[p_id]['SOperW'] = sd[p_id].pop('SO/W')
        else:  # more than one stint
            for stint in sd[p_id].keys():
                sd[p_id][stint].update(pitching_dict[p_id][stint])
                sd[p_id][stint]['BAOpp'] = sd[p_id][stint].pop('BA')
                sd[p_id][stint]['GIDP'] = sd[p_id][stint].pop('GDP')
                sd[p_id][stint]['BFP'] = sd[p_id][stint].pop('BF')
                sd[p_id][stint]['ERAplus'] = sd[p_id][stint].pop('ERA+')
                sd[p_id][stint]['SOperW'] = sd[p_id][stint].pop('SO/W')
    return sd


def reset_table(table='batting', year=cur_season):
    """Clear out all entries w/ yearID = year."""
    """Set table='pitching' to reset that table"""
    mydb = pymysql.connect(host, username, password, lahmandb)
    cursor = mydb.cursor()
    statement = "DELETE FROM %s WHERE yearID = %s" % (table, year)
    cursor.execute(statement)
    mydb.commit()
    cursor.close()


def find_latest_year():
    """Return latest year the db has batting data."""
    mydb = pymysql.connect(host, username, password, lahmandb)
    cursor = mydb.cursor()
    statement = 'SELECT MAX(yearID) from batting'
    cursor.execute(statement)
    latest_year = cursor.fetchone()[0]
    mydb.commit()
    cursor.close()
    return latest_year


def reset_db():
    """Reset database to 2014."""
    """For testing. Leaves in expanded stats."""

    latest_year = find_latest_year()
    for year in range(2015, int(latest_year) + 1):
        for table in ['batting', 'pitching', 'fielding']:
            reset_table(table=table, year=year)
    # reset_master()  # FOR TESTING this is commented out.

    exp_p_columns = ['ROE', 'BAbip', 'OPS', 'SLG', 'OBP', 'WHIP',
                     'ERAplus', 'FIP', 'PA', 'SOperW']

    if 'FIP' in get_columns('pitching'):
        print "Dropping expanded pitching stats."
        print "Dropping mlbID from master"
        statement = "ALTER TABLE pitching "
        for col in exp_p_columns:
            statement += "DROP COLUMN {}, ".format(col)
        statement = statement[:-2]
        statement2 = "ALTER TABLE master DROP COLUMN mlbamID"
        mydb = pymysql.connect(host, username, password, lahmandb)
        cursor = mydb.cursor()
        cursor.execute(statement)
        mydb.commit()
        cursor.execute(statement2)
        mydb.commit()
        cursor.close()

    print "database reset to 2014."


def find_rookies(id_set):  # pass in id_dict, erase lines that generate it
    """Find ids in id_set NOT IN master."""
    rookie_list = []
    mydb = pymysql.connect(host, username, password, lahmandb)
    cursor = mydb.cursor()
    for id_ in id_set:
        statement = "select nameFirst from master where playerid='%s'" % (id_)
        cursor.execute(statement)
        result = cursor.fetchone()
        if not result:
            rookie_list.append(id_)
    mydb.commit()
    cursor.close()
    return rookie_list


def populate_master(rookie_set, year, expanded=True):
    """Insert rookies into master."""
    """Expanded will insert mlb adv med ID into master."""
    ppl_dict = make_people_dict(people_csv)
    cols = get_columns('master')
    # check cols right length
    #   if not insert mlbamID
    if len(cols) == 24 and expanded is True:
        add_mlbamid_master()
        print "trying to add mlbamID to master"
    else:
        assert len(cols) == 25

    missing = ['deathCountry', 'deathState', 'deathCity', 'finalGame']
    dates = ['debut', 'finalGame']
    # empty = ['deathYear', 'deathMonth', 'deathDay']
    # move missing to the end
    for field in missing:
        cols.append(cols.pop(cols.index(field)))

    mydb = pymysql.connect(host, username, password, lahmandb)
    cursor = mydb.cursor()
    statement_start = "INSERT INTO master ("
    for col in cols:
        statement_start += col + ", "
    statement_start = statement_start[:-2] + ") VALUES ("

    for rookie in rookie_set:
        statement = statement_start
        # data from bbref
        a, rookie_data = rookie_deets(rookie)
        # data from chadwick
        try:
            rookie_data.update(ppl_dict[rookie])
        except KeyError:
            print "Chadwick missing data for: " + rookie
        # set finalGame
        rookie_data['finalGame'] = "'" + year + "-12-31 00:00:00'"
        for col in cols:
            datum = rookie_data.get(col, '')
            if datum and datum.isdigit() is False and col not in dates:
                # hack for birthState
                if col == 'birthState' and len(datum) > 2:
                    datum = datum[:2]
                if col == 'nameLast':
                    apos_at = datum.find("'")
                    if apos_at != -1:
                        datum = datum[:apos_at] + "'" + datum[apos_at:]
                statement += "'" + datum + "', "
            elif datum.isdigit() or col in dates:
                statement += datum + ", "
            else:
                statement += 'NULL, '
        statement = statement[:-2] + ")"
        if len(statement) > 550:
            print "too_long: " + statement

        cursor.execute(statement)
        time.sleep(0.5)

    mydb.commit()
    cursor.close()
    return  # statement


def rook_ins_test(ins_statement):
    """Test rookie insert."""
    mydb = pymysql.connect(host, username, password, lahmandb)
    cursor = mydb.cursor()
    cursor.execute(ins_statement)
    mydb.commit()
    cursor.close()


def rookie_deets(rookie_id):
    """Make UPDATE string for rookie update."""
    """Return update_string and data list"""
    update_string = "UPDATE master SET "
    # rookie_id = 'walkela01'
    url_start = "http://www.baseball-reference.com/players/"
    url = url_start + rookie_id[0] + "/" + rookie_id + ".shtml"
    page = urllib2.urlopen(url).read()
    soup = BeautifulSoup(page, 'html.parser')
    carrots = soup.find(id="info_box")
    carrots = carrots.find_all('p')
    carrot_strs = [c.encode('utf-8').strip() for c in carrots]
    right_one = [n for n, c in enumerate(carrot_strs) if c.find("Bats") != -1]
    # carrots = str(carrots[2].get_text())[:-2]  # old line
    if right_one:
        carrots = carrots[right_one[-1]].get_text()[:-2]  # -2 strips off \n
        carrots = carrots.replace("\n", ",")
        carrots = carrots.split(',')
        carrots = [carrot.encode('utf-8').strip() for carrot in carrots]
    else:
        print "something went wrong. Here, have some carrots."
        return carrots
    # bi -> index of bats in carrots.  Mult positions throws this off
    bi = [ndx for ndx, item in enumerate(carrots) if item.startswith('Bats')]
    # print carrots
    bi = bi[0]
    bats = carrots[bi][carrots[bi].find(" ") + 1]
    update_string += "bats='" + bats + "', "
    # deal w/ freakin' ambidextrous pitchers
    if rookie_id != 'vendipa01':
        throws = carrots[bi + 1][carrots[bi + 1].find(" ") + 1]
    else:
        throws = 'B'
        carrots.pop(3)
        dot = carrots[3].find(".")
        carrots[3] = carrots[3][dot + 1:]

    update_string += "throws='" + throws + "', "
    feet = int(carrots[bi + 2][carrots[bi + 2].find(" ") + 1]) * 12
    inches = int(carrots[bi + 2][-2])
    height = str(feet + inches)
    update_string += "height=" + height + ", "
    lbs_start = carrots[bi + 3].find(" ") + 1
    weight = str(carrots[bi + 3][lbs_start:lbs_start + 3])
    update_string += "weight=" + weight + ", "
    dob = soup.find(id='necro-birth')['data-birth']
    dob = dob.split('-')
    dob = [str(date.strip()) for date in dob]
    year, month, day = dob[0], dob[1], dob[2]
    update_string += "birthYear=" + year + ", "
    update_string += "birthMonth=" + month + ", "
    update_string += "birthDay=" + day + ", "
    birth_place = soup.find(id='necro-birth').get_text()
    place_start = birth_place.find("in")
    birth_place = birth_place[place_start + 2:]
    birth_place = birth_place.split(",")
    birth_place = [str(string.strip()) for string in birth_place]
    birth_city, birth_state = birth_place[0], birth_place[1]
    if len(birth_place) == 3:
        birth_country = birth_place[2]
    else:
        birth_country = 'USA'
    update_string += "birthCountry='" + birth_country + "', "
    update_string += "birthState='" + birth_state + "', "
    update_string += "birthCity='" + birth_city + "', "
    debut_text = soup.select('a[href*="dest=debut"]')[0].get_text()
    deb_dates = debut_text.split(' ')
    deb_mon = str(time.strptime(deb_dates[0], '%B').tm_mon)
    deb_day = str(deb_dates[1][:-1])
    deb_year = str(deb_dates[2])
    debut_time = "'" + deb_year + "-" + deb_mon + "-" + deb_day + " 00:00:00'"
    update_string += "debut=" + debut_time + " "
    update_string += "WHERE playerID='" + rookie_id + "'"
    rook_data = {'bats': bats, 'throws': throws, 'height': height,
                 'weight': weight, 'birthYear': year, 'birthMonth': month,
                 'birthDay': day, 'birthCity': birth_city,
                 'playerID': rookie_id, 'birthState': birth_state,
                 'birthCountry': birth_country, 'debut': debut_time}
    return update_string, rook_data


def update_master(rookie_list):
    """Update master table w/ data from bbref player pages."""
    """Sleep timer makes it easier on bbref but slows things down."""
    mydb = pymysql.connect(host, username, password, lahmandb)
    cursor = mydb.cursor()
    for rookie in rookie_list:
        statement, rook_data = rookie_deets(rookie)
        cursor.execute(statement)
        time.sleep(0.5)
    mydb.commit()
    cursor.close()


def reset_master():
    """Delete new entries."""
    latest_year = find_latest_year()
    for year in range(2015, latest_year + 1):
        mydb = pymysql.connect(host, username, password, lahmandb)
        cursor = mydb.cursor()
        statement = """DELETE FROM master
                       where finalGame BETWEEN '{}-12-31 00:00:00' and
                       '{}-12-31 23:59:59'""".format(year, year)
        cursor.execute(statement)
        mydb.commit()
        cursor.close()


def make_paths(position, year=cur_season):
    """Return fielding paths for csv and shtml files."""
    cwd = os.getcwd()
    csv_path = os.path.join(cwd, "data", "data" + year,
                            "fielding_" + position + ".csv")
    shtml_path = os.path.join(cwd, "data", "data" + year,
                              "fielding_" + position + ".shtml")
    return csv_path, shtml_path


if __name__ == '__main__':
    main()
    # pass
