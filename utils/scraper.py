"""Grab bbref data with spynner.

Get csv and html data, store in file
and move to correct location.

Depending on your d/l speed and the speed of baseball-reference's
server you may wish to change the wait_load values.  More time is
slower but you're more likely to get the data.
"""

import spynner
import os
from bs4 import BeautifulSoup
import time
# import pyquery

# base url for testing, need to write script
# to generate url's for other years
# url_start = 'http://www.baseball-reference.com/leagues/MLB/'
# url_end = '-standard-batting.shtml'

year = '2016'

# example of changing browser, not in use
# useragent = "Mozilla/5.0 (Windows NT 6.1; rv:7.0.1)
# Gecko/20100101 Firefox/7.0.1"


def url_maker(fielding=False, year=year):
    """Return list of tuples (name, url) for a year."""
    """Fielding is time consuming, False by default"""
    """Adjust name to whatever you like."""
    url_start = 'http://www.baseball-reference.com/leagues/MLB/'
    url_start += str(year)
    name_url_pairs = []
    # make batting tuple
    bats_url = url_start + '-standard-batting.shtml'
    name_url_pairs.append(('bats', bats_url))

    # make pitching tuples
    arms_url = url_start + '-standard-pitching.shtml'
    name_url_pairs.append(('arms', arms_url))
    arms_extra_url = url_start + '-batting-pitching.shtml'
    name_url_pairs.append(('arms_extra', arms_extra_url))

    # make fielding tuples
    if fielding is True:
        positions = ['p', 'c', '1b', '2b', '3b', 'ss', 'rf', 'lf', 'cf']
        url_start += '-specialpos_'
        for pos in positions:
            url = url_start + pos + '-fielding.shtml'
            name_url_pairs.append(('fielding_' + pos, url))

    return name_url_pairs


def check_files(year=year, expiry=1, fielding=False, chadwick=False):
    """Verify files in data are new and of expected sizes."""
    """Set expiry to ensure that files are only 'expiry' days old."""
    """Intended to be run as import from main dir.  Paths are affected."""
    past_due, exists = False, True
    data_dir = os.path.join('', 'data', 'data' + str(year))
    # print data_dir
    if os.path.isdir(data_dir) is False:
        print "Data dir for " + str(year) + " is missing."
        return True, False  # past_due, exists  # nb, uglyish
    f_names = os.listdir(data_dir)
    arms_bats = [i for i in f_names if (i.find('arms') + i.find('bats')) != -2]
    to_check = [] + arms_bats
    assert len(arms_bats) == 6

    chad_csv = ['chadwick.csv']

    if fielding is True:
        gloves = [i for i in f_names if i.find('fielding') > -1]
        # print len(gloves)
        assert len(gloves) == 18  # will be 18
        to_check += gloves
    if chadwick is True:
        assert os.path.isfile(os.path.join(data_dir, 'chadwick.csv'))
        to_check += chad_csv

    now = time.time()
    for f in to_check:
        # nb, this if block is likely redundant
        if os.path.isfile(os.path.join(data_dir, f)) is False:
            exists = False
            print f + " not found."
            return True, False  # past_due, exists  # nb, ugly
        created = os.path.getmtime(os.path.join(data_dir, f))
        age = now - created
        if age > 3600 * 24 * expiry:
            # print "Alert: " + f + " is more than {} day(s) old.".format(expiry)
            past_due = True

    # check file size is > arbitrary value
    paths = [os.path.join(data_dir, i) for i in to_check]
    for path in paths:
        assert os.path.getsize(path) > 0
        if os.path.getsize(path) < 10000:
            print "Alert: " + path + " is very small."
    if past_due:
        print "Alert: files are more than {} day(s) old.".format(expiry)
    return past_due, exists


def get_all_data(year=year, expiry=1, fielding=False, chadwick=False):
    """Grab all data and write core files."""
    """Options for fielding data and bio data for rookies/master."""
    name_url_pairs = url_maker(year=year, fielding=fielding)
    # loop over tuples and get_dats
    for pair in name_url_pairs:
        get_data(pair[1], pair[0], year)
    # either do chadwick or not
    if chadwick is True:
        get_biographical()
    # Check if data is there, new and in range of len
    past_due, exists = check_files(year, expiry, fielding=fielding, chadwick=chadwick)
    if past_due is False and exists is True:
        print "Files now up to date."
    return past_due, exists


def get_data(url, name, year):
    """Grab one csv and one html from bbref url and write to data folder."""
    """Will overwrite existing files.
    Runtime is about 30sec per page.
    -> Fielding data would take about 5mins.
    """
    # make the paths for the files
    # and the directory if needed
    # cwd = os.path.getcwd()
    stats_dir = os.path.join('', 'data', 'data' + str(year))
    if os.path.isdir(stats_dir) is False:
        print "Need to make directory."
        os.mkdir(stats_dir)
    csv_path = os.path.join(stats_dir, name + '.csv')
    html_path = os.path.join(stats_dir, name + '.shtml')
    # start up spynner
    br = spynner.Browser()
    # url = url_start + year + url_end

    # the page takes a while to load, 10 was too little
    br.load(url, load_timeout=15)
    br.create_webview()
    # may want to get rid of show() - nice for debugging.
    br.show()
    print "Processing " + name
    br.load_jquery(True)
    # unhide non-qualifiers
    try:
        br.click('input[type="checkbox"]')
        br.wait_load(10)
    except spynner.SpynnerTimeout:
        print "timed out."
        # raise spynner.SpynnerTimeout

    # grab the html before changing to csv

    with open(html_path, 'w') as f:
        f.write(br.html.encode('utf-8'))

    print "HTML written."
    # convert table to csv on page. Will be contained in only <pre>
    # fielding query ('span[tip$="values"]')[2] # third of 4
    # for catchers (or fielders in general):

    if url.find('fielding') == -1:
        br.runjs("""jQuery('span[tip$="values"]:last').click()""")
        br.wait(5)
    else:
        # this is nice and specific and should work for all postions, but doesn't.
        # jq = """jQuery('span[tip$="values"][onclick^="table2csv(\'players_standard"]:first').click()"""

        # catchers return extra values
        if name[-1] != 'c':
            br.runjs("""jQuery('span[tip$="values"]:eq(2)').click()""")  # old line
        else:
            br.runjs("""jQuery('span[tip$="values"]:eq(3)').click()""")  # old line

        # br.runjs("""jQuery('span[tip$="values"][onclick^="table2csv(\'players_standard"]:first').click()""")

        '''if br.runjs(query + '.length') > 1:
            query += "[0]"

        br.runjs(query + ".click()")
        br.wait(5)'''

    soup = BeautifulSoup(br.html, 'html.parser')
    # try this
    # soup = soup.decode('utf-8', 'ignore')
    pre_section = soup.find('pre').get_text()
    # print "Type of pre_section: ",
    # print type(pre_section)
    # and try this too.
    pre_section = pre_section.encode('ascii', 'replace')
    pre_section = pre_section.replace('?', ' ')
    with open(csv_path, 'w') as p:
        p.write(pre_section.encode('utf-8'))

    print "CSV written."
    # f(x) freezing in REPL after successful execution
    # tried the two lines below; didn't help
    # br.destroy_webview()
    # print "050"
    try:
        br.destroy_webview()
        br.close()
    except AttributeError, e:
        print "Spynner problem closing browser." + e
    # br.close()
    # print "060"
    return None


def get_biographical():
    """D/L biographical data from Chadwick Bureau."""
    url = 'https://raw.githubusercontent.com/chadwickbureau/'
    url += 'register/master/data/people.csv'
    print "Getting Chadwick Bureau data."
    print "~35MB file. This may take a minute."
    chad_path = os.path.join('', 'data', 'data' + year)
    browser = spynner.Browser()
    cb_csv = browser.download(url)
    with open(os.path.join(chad_path, 'chadwick.csv'), 'w') as cb:
        cb.write(cb_csv.encode('utf-8'))
    return
