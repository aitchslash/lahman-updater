"""
This reads the 2015 MLB hitting stats from a csv from bbref.

In order to extract the bbref ID's a copy of the html will
have to be scraped as well.

Notes:
1) Adding mlb advanced media ID as default.  Using INT(11)
2) Ensure that "Hide non-qualifiers for rate stats" is the same on all docs
3) Stint is not assured to be correct. Relies on bbref order
4) no recorded out leads to an infinite ERA - using 99.99
5) TeamID's for Chicago teams seem odd (Cubs = CHA, WS = CHN)
:   seems only to be for 2013 and 2014 (only looked at 2014db)
6) Setting new players last game to year-12-31 so they're easy to find
7) Rookie insert take a while on first run
8) Rookie birthState truncated to 2 chars
:   may want to expand column to accept more
:   or find converter



ToDo:
test new decorator
:   if good, remove many fix_mismatches
:   and change sd_orig

maybe make fix_csv into a decorator
:   might slow things down but would save code

check inner f(x) in expanded pitching stats_dict
:   currently broken - find out why
get rid of extra print statements

consider making fix_mismatches into a decorator

check fielding for aj pierzynski and rusney castillo - looks fine
reset_db and test again -- looks good
delete old code and print statements
improve comments and doc strings
update readme


rework rookie_deets to insert rather than update - done

test pitching string maker - done section
delete 2015 data
fix columns
run insert of pitching data

use new p_batting_against csv - make/dict- done
alter p table to include new stats - fip, maybe more from orig
    may need a table reset f(x)

once working remove old code

examine batting stats on bbref to maybe get more

consider getting WAR

look into automating getting csv/source files via spynner

update pitching table, take get_headers out of loop

roll inserts into one f(x) - main
create field_length dictionary with header

consider reworking insert_batter & insert_pitcher
:   maybe use a decorator for the statement_start/end?
:   first lines in ss the same
:   if stint['Tm'] = 'TOT', now likely unneccessary
open lahman15 release
:   check inf(inity extracted) pitching
:   check pitching for missing stats (not in mine or bbref) - SH, S, GIDP
    check teamID's for Chicago

check if extract_page and get_ids are ever used seperately
:   roll into one if not

"""

import csv
from bs4 import BeautifulSoup
import pymysql
import os
import urllib2
from time import strptime
from time import sleep
import pprint  # nb, just for test prints

lahmandb = "lahmandb"

bats_csv = 'bbref_2015_batting.csv'
arms_csv = 'bbref_2015_pitching.csv'
arms_extra_csv = 'p_batting_against.csv'
bats_html = 'bbref_html.shtml'
arms_html = 'bbref_arms_html.shtml'
arms_extra_html = 'p_batting_against.shtml'
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


def add_pitching_columns():
    """Run once.  Add extra stat columns to pitching."""
    mydb = pymysql.connect('localhost', 'root', '', lahmandb)
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
    mydb = pymysql.connect('localhost', 'root', '', lahmandb)
    cursor = mydb.cursor()
    statement = """ALTER TABLE master
                   ADD COLUMN mlbamID INT(11) DEFAULT NULL"""
    cursor.execute(statement)
    mydb.commit()
    cursor.close()


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

        # rename keys to match lahman
        for i in range(0, len(header)):
            if header[i].find('key_') == 0:
                header[i] = header[i][4:] + "ID"
        for i in range(0, len(header)):
            j = header[i].find("_")
            if j != -1:
                header[i] = header[i][:j] + header[i][j + 1].upper() + header[i][j + 2:]

        print header
        for row in reader:
            # people_dict
            # limit dictionary to those youger than 50
            if row['birthYear'] and int(row['birthYear']) > int(year) - 50:
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
        mydb = pymysql.connect('localhost', 'root', '', lahmandb)
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
    return inner


@fix_mismatches
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
            # fix Putouts/Pickoffs for P fielding
            header[-1] = 'PK'
            print header
            # field_len = 25 # 35 caused headaches

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
    mydb = pymysql.connect('localhost', 'root', '', lahmandb)
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
    mydb = pymysql.connect('localhost', 'root', '', lahmandb)
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


def setup(expanded=True):
    """Run one-time queries and audit data."""
    b_soup = extract_page(bats_html)
    p_soup = extract_page(arms_html)
    ids = get_ids(b_soup)
    p_ids = get_ids(p_soup)
    # ids.update(p_ids)
    # add rookies
    fix_csv(bats_csv)
    batting_dict = make_bbrefid_stats_dict(bats_csv, ids, table='batting')
    # batting_dict = fix_mismatches(batting_dict)
    fix_csv(arms_csv)
    # old lines, might be useful if not adding expanded data
    pitching_dict = make_bbrefid_stats_dict(arms_csv, p_ids, table='pitching')
    # pitching_dict = fix_mismatches(pitching_dict)
    # a, pitching_dict, c = expand_p_test()
    if expanded is True:
        pitching_dict = expand_pitch_stats(pitching_dict)
    # team_dict = make_team_dict()
    batting_cols = get_columns('batting')
    pitching_cols = get_columns('pitching')
    # if pitching cols need to be added i.e. != 40 or == 30
    #   add_pitching_cols
    id_set = set()
    id_set.update(pitching_dict.keys())
    id_set.update(batting_dict.keys())
    rookie_set = set(find_rookies(id_set))
    populate_master(rookie_set)
    ins_table_data(batting_dict, batting_cols, table='batting')
    ins_table_data(pitching_dict, pitching_cols, table='pitching')
    ins_fielding()
    # batting_dict, team_dict, batting_cols, pitching_dict, pitching_cols
    return


def ins_table_data(table_data, cols_array, table='batting'):
    """Insert table data for batting or pitching."""
    team_dict = make_team_dict()
    if table == 'batting':
        insert_player = insert_batter
    else:
        insert_player = insert_pitcher
    print 'setup run, opening db...'
    print 'inserting into ' + table + "..."
    mydb = pymysql.connect('localhost', 'root', '', lahmandb)
    cursor = mydb.cursor()
    for key in table_data.keys():
        statements = insert_player(key, table_data, team_dict, cols_array)
        for statement in statements:
            cursor.execute(statement)
    mydb.commit()
    cursor.close()
    return


# key will be bbref_dict.keys() i.e. bbref_id
def insert_batter(key, stats_dict, team_dict, fields_array):
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
        # pos_dict = fix_mismatches(pos_dict)
        print 'inserting into ' + "fielding " + pos + " ..."
        mydb = pymysql.connect('localhost', 'root', '', lahmandb)
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
            # print stats
            # print key
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
    # if len(stats) == 35:  # only one stint # old line
    # if len(stats) == 52:  # only one stint
    if len(stats) > 10:
        stats['stint'] = 1
        stints.append(stats)
    else:
        for stint_key in stats.keys():
            stats[stint_key]['stint'] = str(stint_key[-1])
            stints.append(stats[stint_key])

    # move IPouts to end of array
    fields_array.append(fields_array.pop(fields_array.index('IPouts')))
    # print field

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
        # update = ['BAopp', 'GIDP', 'SF', 'SH', 'OBP',
        # 'OPS', 'ROE', 'SLG', 'BAbip']
        """
        stat_keys = ['W', 'L', 'G', 'GS', 'CG', 'SHO', 'SV', 'H',
                     'ER', 'HR', 'BB', 'SO', 'ERA', 'IBB', 'WP', 'HBP',
                     'BK', 'BFP', 'GF', "R",
                     'BAopp', 'PA', 'WHIP', 'SOperW', 'FIP', 'ERAplus', 'ROE',
                     'BAbip', 'SLG', 'GIDP', 'SF', 'SH', 'OBP', 'OPS', 'SLG']
        """
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


def expand_pitch_stats(pitching_dict):
    """Add new stats to pitching_dict."""
    soup = extract_page(arms_extra_html)
    # soup_orig = extract_page(arms_html)
    ids = get_ids(soup)
    # ids_orig = get_ids(soup_orig)
    fix_csv(arms_extra_csv)
    # pitching extra has len=30, same as default
    sd = make_bbrefid_stats_dict(arms_extra_csv, ids)
    # sd = fix_mismatches(sd)
    # sd_orig = fix_mismatches(pitching_dict)
    sd_orig = pitching_dict

    # make sure the two pages match up
    print len(sd.keys())
    print len(sd_orig.keys())
    assert sd.keys() == sd_orig.keys()

    def change_key_names(stint):
        """Change keys to match lahman columns."""
        stint['BAOpp'] = stint.pop('BA')
        stint['GIDP'] = stint.pop('GDP')
        stint['BFP'] = stint.pop('BF')
        stint['ERAplus'] = stint.pop('ERA+')
        stint['SOperW'] = stint.pop('SO/W')

    for p_id in sd.keys():
        if len(sd[p_id].keys()) > 10:  # only one stint
            sd[p_id].update(sd_orig[p_id])
            change_key_names(sd[p_id])
            '''
            sd[p_id]['BAOpp'] = sd[p_id].pop('BA')
            sd[p_id]['GIDP'] = sd[p_id].pop('GDP')
            sd[p_id]['BFP'] = sd[p_id].pop('BF')
            sd[p_id]['ERAplus'] = sd[p_id].pop('ERA+')
            sd[p_id]['SOperW'] = sd[p_id].pop('SO/W')'''
        else:  # more than one stint
            for stint in sd[p_id].keys():
                sd[p_id][stint].update(sd_orig[p_id][stint])
                change_key_names[stint](sd[p_id])
                '''
                sd[p_id][stint]['BAOpp'] = sd[p_id][stint].pop('BA')
                sd[p_id][stint]['GIDP'] = sd[p_id][stint].pop('GDP')
                sd[p_id][stint]['BFP'] = sd[p_id][stint].pop('BF')
                sd[p_id][stint]['ERAplus'] = sd[p_id][stint].pop('ERA+')
                sd[p_id][stint]['SOperW'] = sd[p_id][stint].pop('SO/W')
                '''
    return sd


def reset_db(table='batting', year=year):
    """Clear out all entries w/ yearID = year."""
    """Set table='pitching' to reset that table"""
    mydb = pymysql.connect('localhost', 'root', '', lahmandb)
    cursor = mydb.cursor()
    statement = "DELETE FROM %s WHERE yearID = %s" % (table, year)
    cursor.execute(statement)
    mydb.commit()
    cursor.close()


def find_rookies(id_set):  # pass in id_dict, erase lines that generate it
    """Find ids in id_set NOT IN master."""
    rookie_list = []
    mydb = pymysql.connect('localhost', 'root', '', lahmandb)
    cursor = mydb.cursor()
    for id_ in id_set:
        statement = "select nameFirst from master where playerid='%s'" % (id_)
        cursor.execute(statement)
        result = cursor.fetchone()
        if not result:
            rookie_list.append(id_)
    return rookie_list


def populate_master(rookie_set, expanded=True):
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

    mydb = pymysql.connect('localhost', 'root', '', lahmandb)
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
        rookie_data.update(ppl_dict[rookie])
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
        sleep(0.5)

    mydb.commit()
    cursor.close()
    return  # statement


def rook_ins_test(ins_statement):
    """Test rookie insert."""
    mydb = pymysql.connect('localhost', 'root', '', lahmandb)
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
    # print rookie_id
    # print carrots[2]
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
    # print rookie_id,
    # print carrots
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
    deb_mon = str(strptime(deb_dates[0], '%B').tm_mon)
    deb_day = str(deb_dates[1][:-1])
    deb_year = str(deb_dates[2])
    debut_time = "'" + deb_year + "-" + deb_mon + "-" + deb_day + " 00:00:00'"
    update_string += "debut=" + debut_time + " "
    update_string += "WHERE playerID='" + rookie_id + "'"
    # pprint.pprint(update_string)
    rook_data = {'bats': bats, 'throws': throws, 'height': height,
                 'weight': weight, 'birthYear': year, 'birthMonth': month,
                 'birthDay': day, 'birthCity': birth_city,
                 'playerID': rookie_id, 'birthState': birth_state,
                 'birthCountry': birth_country, 'debut': debut_time}
    return update_string, rook_data


def update_master(rookie_list):
    """Update master table w/ data from bbref player pages."""
    """Sleep timer makes it easier on bbref but slows things down."""
    # rookie_list = rookies_to_update()
    mydb = pymysql.connect('localhost', 'root', '', lahmandb)
    cursor = mydb.cursor()
    for rookie in rookie_list:
        statement, rook_data = rookie_deets(rookie)
        # print statement
        cursor.execute(statement)
        sleep(0.5)
    mydb.commit()
    cursor.close()


def reset_master():
    """Delete new entries."""
    mydb = pymysql.connect('localhost', 'root', '', lahmandb)
    cursor = mydb.cursor()
    statement = """DELETE FROM master
                   where finalGame BETWEEN '2015-12-31 00:00:00' and
                   '2015-12-31 23:59:59'"""
    cursor.execute(statement)
    mydb.commit()
    cursor.close()


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
