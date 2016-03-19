"""
This reads the 2015 MLB hitting stats from a csv from bbref.

In order to extract the bbref ID's a copy of the html will
have to be scraped as well.

Notes:
1) had to erase blank first line in bbref_2015_batting - taken care of
2) Ensure that "Hide non-qualifiers for rate stats" is the same on all docs
3) Stint is not assured to be correct. Relies on bbref order
4) BAopp for pitchers lacks SH, S and GIDP so is inexact
5) SH, S, and GIDP missing from pitching
6) no recorded out leads to an infinite ERA - using 99.99
7) TeamID's for Chicago teams seem odd (Cubs = CHA, WS = CHN)
:   seems only to be for 2013 and 2014 (only looked at 2014db)

ToDo:
comprehension for fix_csv
put up on github

remove test f(x)'s
roll inserts into one f(x) - main
create field_length dictionary with header

consider reworking insert_batter & insert_pitcher
:   first lines in ss the same
:   if stint['Tm'] = 'TOT', now likely unneccessary
open lahman15 release
:   check inf(inity extracted) pitching
:   check pitching for missing stats (not in mine or bbref) - SH, S, GIDP
    check teamID's for Chicago

check if extract_page and get_ids are ever used seperately
:   roll into one if not

check names correspond bbref_id:lahman_id --- done!
: *** aah, but new_ids should be added to master ***
:   <s>those that don't likely need a new_entry f(x)</s> use assigned bbrefID
:   collect rookie id's and import name data from chadwick
:       could use non_match array in make_bbrefid_stats_dict
"""

import csv
from bs4 import BeautifulSoup
import pymysql
import os
import urllib2
import pprint  # nb, just for test prints

bats_csv = 'bbref_2015_batting.csv'
arms_csv = 'bbref_2015_pitching.csv'
bats_html = 'bbref_html.shtml'
arms_html = 'bbref_arms_html.shtml'
year = '2015'
people_csv = 'people.csv'

# could generate field_length dict from len(header)
field_length = {'batting': 30,
                'pitching': 35,
                'C': 29,
                'SS': 24,
                '2B': 24,
                'RF': 25,
                'LF': 25,
                'CF': 25,
                'P': 25,
                '1B': 25,
                '3B': 25}


def extract_page(page):
    """Turn html page into a BeautifulSoup."""
    with open(page, "r") as html:
        soup = BeautifulSoup(html, 'html.parser')
    return soup


def get_ids(soup):
    """Return a dictionary mapping {name:bbref_id}."""
    name_bbref_dict = {}
    tags = soup.select('a[href^=/players/]')
    for tag in tags:
        # format and deal w/ unicode
        name = (tag.parent.text)
        name = name.encode('ascii', 'replace')  # .decode('ascii')
        name = name.replace('?', ' ')
        if name[-1].isalnum() is False:
            name = name[:-1]
        if name.find(' ') == -1:  # error checking
            pprint.pprint(name)
        addy = str(tag['href'])
        bbref_id = addy[addy.rfind('/') + 1: addy.rfind('.')]
        name_bbref_dict[name] = bbref_id
    return name_bbref_dict


def make_people_dict(people_csv):
    """Return dictionary mapping bbref to master table data."""
    """Data from chadwick bureau."""
    people_dict = {}
    with open(people_csv, 'rb') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        print header  # just for pep-8
        for row in reader:
            people_dict
            # limit dictionary to those youger than 50
            if row['birth_year'] and int(row['birth_year']) > int(year) - 50:
                bbref_id = row['key_bbref']
                people_dict[bbref_id] = row
        return people_dict


def make_bbrefid_stats_dict(bbref_csv, name_bbref_dict, table='batting'):
    """Make dictionary mapping bbrefID (as key) to extracted stats (values)."""
    """multiple teams stats are nested as stints"""

    # set field length for data.  If > then multiple stints
    field_len = field_length[table]

    stats_dict = {}
    non_match = []
    with open(bbref_csv, "rb") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        print (header)  # nb, just to appease pep-8
        if table == 'P':
            # fix Putouts/Pickoffs
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
                    elif len(stats_dict[bbref_id]) == field_len:
                        first_stint = stats_dict[bbref_id]
                        second_stint = row
                        del stats_dict[bbref_id]
                        stats_dict[bbref_id] = {'stint1': first_stint,
                                                'stint2': second_stint}
                    # then stints have been used, add this one
                    else:
                        stint_num = "stint" + str(len(stats_dict[bbref_id]) + 1)
                        stats_dict[bbref_id].update({stint_num: row})

                except:
                    if row['Name'] != 'Name':
                        non_match.append(row['Name'])

    print "non-match: ",
    print non_match  # warning
    return stats_dict


def get_columns(table):
    """Get column names from Lahman table (pitching/batting)."""
    mydb = pymysql.connect('localhost', 'root', '', 'lahman14')
    cursor = mydb.cursor()
    statement = "SHOW columns FROM %s" % (table)
    print statement  # nb, test line
    cursor.execute(statement)
    query_results = cursor.fetchall()
    cursor.close()
    columns = []
    for elem in query_results:
        columns.append(elem[0])
    return columns


def make_team_dict():
    """Return dictionary mapping team_ids, lahman to bbref."""
    mydb = pymysql.connect('localhost', 'root', '', 'lahman14')
    cursor = mydb.cursor()
    statement = '''SELECT teamID, teamIDBR
                   FROM teams
                   WHERE yearID = 2014'''
    cursor.execute(statement)
    team_tuples = cursor.fetchall()
    cursor.close()
    team_dict = {}
    for team in team_tuples:
        team_dict[team[1]] = team[0]
    return team_dict


def setup():
    """Run one-time queries and audit data."""
    soup = extract_page(bats_html)
    ids = get_ids(soup)
    batting_dict = make_bbrefid_stats_dict(bats_csv, ids, table='batting')
    batting_dict = fix_mismatches(batting_dict)
    pitching_dict = make_bbrefid_stats_dict(arms_csv, ids, table='pitching')
    pitching_dict = fix_mismatches(pitching_dict)
    team_dict = make_team_dict()
    batting_fields = get_columns('batting')
    pitching_fields = get_columns('pitching')
    return batting_dict, team_dict, batting_fields, pitching_dict, pitching_fields


def ins_table_data(table='batting'):
    """Insert table data."""
    batting_dict, team_dict, batting_fields, pitching_dict, pitching_fields = setup()
    if table == 'batting':
        table_data = batting_dict
        fields_array = batting_fields
        insert_player = insert_batter
    else:
        table_data = pitching_dict
        fields_array = pitching_fields
        insert_player = insert_pitcher
    print 'setup run, opening db...'
    print 'inserting into ' + table + "..."
    mydb = pymysql.connect('localhost', 'root', '', 'lahman14')
    cursor = mydb.cursor()
    for key in table_data.keys():
        statements = insert_player(key, table_data, team_dict, fields_array)
        for statement in statements:
            # print statement
            cursor.execute(statement)
    mydb.commit()
    cursor.close()
    return


# key will be bbref_dict.keys() i.e. bbref_id
def insert_batter(key, stats_dict, team_dict, fields_array):
    """Create insertion string(s)."""
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


def ins_fielding():
    """Insert fielding stats into db."""
    positions = ['P', 'C', '1B', '2B', '3B', 'SS', 'RF', 'LF', 'CF']

    team_dict = make_team_dict()
    cols = get_columns('fielding')

    print 'setup run, opening db...'

    for pos in positions:
        csv_path, html_path = make_paths(pos)
        fix_csv(csv_path)
        soup = extract_page(html_path)
        pos_data = get_ids(soup)
        pos_dict = make_bbrefid_stats_dict(csv_path, pos_data, pos)
        pos_dict = fix_mismatches(pos_dict)
        print 'inserting into ' + "fielding " + pos + " ..."
        mydb = pymysql.connect('localhost', 'root', '', 'lahman14')
        cursor = mydb.cursor()
        for key in pos_dict.keys():
            statements = insert_fielder(key, pos_dict, team_dict, cols, pos)
            for statement in statements:
                cursor.execute(statement)
        mydb.commit()
        print "data committed"
        cursor.close()
    return


def insert_fielder(key, stats_dict, team_dict, fields_array, position):
    """Make insert string for fielding."""
    stats = stats_dict[key]
    stints = []
    field_len = field_length[position]

    if len(stats) == field_len:  # only one stint
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
        if stint['Tm'] != 'TOT':
            statement_start = "INSERT INTO fielding ("
            for field in fields_array:
                statement_start += field + ", "
            statement_start = statement_start[:-2] + ") VALUES ("
            ss = statement_start
            # print key  # test print
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
        else:
            # nb, skipping year totals.  Could use stints and verify.
            continue
    return insert_strings


def insert_pitcher(key, stats_dict, team_dict, fields_array):
    """Create insertion string(s)."""
    """Returns an array of sql commands to be executed."""
    stats = stats_dict[key]
    stints = []
    if len(stats) == 35:  # only one stint
        stats['stint'] = 1
        stints.append(stats)
    else:
        for stint_key in stats.keys():
            stats[stint_key]['stint'] = str(stint_key[-1])
            stints.append(stats[stint_key])

    # move IPouts and BAOpp to end of array
    fields_array.append(fields_array.pop(fields_array.index('IPouts')))
    fields_array.append(fields_array.pop(fields_array.index('BAOpp')))

    insert_strings = []
    empty_warning = set()  # nb, likely served its purpose
    for stint in stints:
        statement_start = "INSERT INTO pitching ("
        for field in fields_array:
            statement_start += field + ", "
        statement_start = statement_start[:-2] + ") VALUES ("
        ss = statement_start
        # print key  # test print
        ss += "'" + key + "', "
        ss += year + ", "
        ss += str(stint['stint']) + ", "
        ss += "'" + team_dict[stint["Tm"]] + "', "
        ss += "'" + stint['Lg'] + "', "
        stat_keys = ['W', 'L', 'G', 'GS', 'CG', 'SHO', 'SV', 'H',
                     'ER', 'HR', 'BB', 'SO', 'ERA', 'IBB', 'WP', 'HBP',
                     'BK', 'BF', 'GF', "R"]
        for sk in stat_keys:
            if stint[sk]:
                if stint[sk] != 'inf':  # ERA = infinity
                    ss += stint[sk] + ', '
                else:
                    ss += '99.99, '  # arbitrarily set ERA to 99.99
            else:
                ss += '0, '
                empty_warning.add(key)  # nb, this is likely done too.
        # NULL for sh, sf, and gidp
        ss += "NULL, NULL, NULL, "
        # lines for IPouts
        last_digit = str(float(stint['IP']))[-1]
        ipouts = int(float(stint['IP'])) * 3 + int(last_digit)
        ss += str(ipouts) + ', '
        # lines for BAopp
        baopp = float(stint['H']) / (int(stint['BF']) - int(stint['HBP']) -
                                     int(stint['BB']) - int(stint['IBB']))
        ss += str('%.3f' % baopp)[1:] + ")"
        insert_strings.append(ss)

    if empty_warning:  # nb, can likely get rid of this
        pprint.pprint(empty_warning)
    return insert_strings


def reset_db(table='batting', year=year):
    """Clear out all entries w/ yearID = year."""
    """Set table='pitching' to reset that table"""
    mydb = pymysql.connect('localhost', 'root', '', 'lahman14')
    cursor = mydb.cursor()
    statement = "DELETE FROM %s WHERE yearID = %s" % (table, year)
    cursor.execute(statement)
    mydb.commit()
    cursor.close()


def find_rookies():
    """Return set of bbrefIDs not in Master."""
    """Tables need to be populated first."""
    tables = ['batting', 'pitching', 'fielding']
    rookie_set = set()
    mydb = pymysql.connect('localhost', 'root', '', 'lahman14')
    cursor = mydb.cursor()
    for table in tables:
        statement = """SELECT %s.playerID
                       FROM %s
                       left join master
                       on %s.playerID=master.playerID
                       where master.playerID is null
                       and %s.yearID=%s""" % (table, table, table, table, year)
        cursor.execute(statement)
        rookies = cursor.fetchall()
        # make rookies a set and format nicely
        # rookies = set(map((lambda x: x[0]), rookies))  # this works
        rookies = set([x[0] for x in rookies])
        problem_ids = [b_id for b_id in rookies if b_id.isalnum() is False]
        if problem_ids:
            print "Problem Ids: ",
            print problem_ids
        rookie_set.update(rookies)
    cursor.close()
    return rookie_set


def update_master(rookie_set):
    """Insert rookies into master."""
    ppl_dict = make_people_dict(people_csv)
    cols = get_columns('master')
    mydb = pymysql.connect('localhost', 'root', '', 'lahman14')
    cursor = mydb.cursor()
    statement_start = "INSERT INTO master ("
    for col in cols:
        statement_start += col + ", "
    for rookie in rookie_set:
        statement = statement_start[:-2] + ") VALUES ("
        statement += "'" + rookie + "', "
        statement += ppl_dict[rookie]['birth_year'] + ", "
        statement += ppl_dict[rookie]['birth_month'] + ", "
        statement += ppl_dict[rookie]['birth_day'] + ", "
        statement += "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
        statement += "'" + ppl_dict[rookie]['name_first'] + "', "
        if "'" not in ppl_dict[rookie]['name_last']:
            statement += "'" + ppl_dict[rookie]['name_last'] + "', "
        else:
            last = ppl_dict[rookie]['name_last']
            apos_at = last.find("'")
            surname_hack = last[:apos_at] + "'" + last[apos_at:]
            statement += "'" + surname_hack + "', "
        statement += "'" + ppl_dict[rookie]['name_given'] + "', "
        statement += "NULL, NULL, NULL, NULL, "
        statement += "'" + ppl_dict[rookie]['mlb_played_first'] + "-01-01 00:00:00', "
        statement += "'" + ppl_dict[rookie]['mlb_played_last'] + "-12-31 00:00:00', "
        statement += "'" + ppl_dict[rookie]['key_retro'] + "', "
        statement += "'" + ppl_dict[rookie]['key_bbref'] + "')"
        # print statement
        cursor.execute(statement)
    mydb.commit()
    cursor.close()
    return


def rookie_deets(rookie_list):
    """Test getting biographical info."""
    rookie = 'badenbu01'
    url_start = "http://www.baseball-reference.com/players/"
    url = url_start + rookie[0] + "/" + rookie + ".shtml"
    print url
    page = urllib2.urlopen(url).read()
    soup = BeautifulSoup(page, 'html.parser')
    carrots = soup.find(id="info_box")
    carrots = carrots.find_all('p')
    carrots = str(carrots[2].get_text())[:-2]
    carrots = carrots.replace("\n", ",")
    carrots = carrots.split(',')
    # ['Position: Pitcher', 'Bats: Right', ' Throws: Right', 'Height: 6\' 2"', ' Weight: 230 lb']
    carrots = [carrot.strip() for carrot in carrots]
    print carrots
    bats = carrots[1][carrots[1].find(" ") + 1]
    print bats
    throws = carrots[2][carrots[2].find(" ") + 1]
    print throws
    feet = int(carrots[3][carrots[3].find(" ") + 1]) * 12
    inches = int(carrots[3][-2])
    height = feet + inches
    print height
    lbs_start = carrots[4].find(" ") + 1
    weight = carrots[4][lbs_start:lbs_start + 3]
    print weight
    dob = soup.find(id='necro-birth')['data-birth']
    print dob
    birth_place = soup.find(id='necro-birth').get_text()
    place_start = birth_place.find("in")
    birth_place = birth_place[place_start + 2:]
    birth_place = birth_place.split(",")
    birth_place = [str(string.strip()) for string in birth_place]
    print birth_place

    return page, soup, carrots


def reset_master():
    """Delete entries with bad dates."""
    mydb = pymysql.connect('localhost', 'root', '', 'lahman14')
    cursor = mydb.cursor()
    statement = """DELETE FROM master where debut < '1800-01-01 00:00:00' and
                   birthYear > 1960"""
    cursor.execute(statement)
    mydb.commit()
    cursor.close()


def fix_mismatches(stats_dict):
    """Fix mismatches between bbref and lahman ids."""
    mydb = pymysql.connect('localhost', 'root', '', 'lahman14')
    cursor = mydb.cursor()
    statement = '''SELECT playerID, bbrefID
                   FROM master
                   WHERE birthYear > %s AND
                   DATE(finalGame) > "%s-1-1" AND
                   playerID != bbrefID''' % (str(int(year) - 50),
                                             str(int(year) - 5))
    cursor.execute(statement)
    mismatches = cursor.fetchall()

    for mm in mismatches:
        if mm[1] in stats_dict.keys():
            stats_dict[mm[0]] = stats_dict[mm[1]]
            print stats_dict[mm[0]]['Name']
            del stats_dict[mm[1]]

    cursor.close()
    return stats_dict


def fix_csv(csv_file):
    """Remove blank line(s) from csv."""
    f = open(csv_file, "r+")
    lines = f.readlines()
    f.seek(0)
    for line in lines:
        if line != "\n":
            f.write(line)
    f.truncate()
    f.close()


def make_paths(position):
    """Return paths for csv and shtml files."""
    cwd = os.getcwd()
    csv_path = os.path.join(cwd, "fielding_" + year,
                            "bbref_" + position + "_fielding.csv")
    shtml_path = os.path.join(cwd, "fielding_" + year,
                              "bbref_" + position + "_fielding.shtml")
    return csv_path, shtml_path
