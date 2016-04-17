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
    url_start += year
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


def check_files(expiry=1, fielding=False, chadwick=False):
    """Verify files in data are new and of expected sizes."""
    """Set expiry to ensure that files are only 'expiry' days old."""
    """Intended to be run as import from main dir.  Paths are affected."""
    data_dir = os.path.join('', 'data', 'data' + year)
    f_names = os.listdir(data_dir)
    arms_bats = [i for i in f_names if (i.find('arms') + i.find('bats')) != -2]
    to_check = [] + arms_bats
    assert len(arms_bats) == 6
    gloves = [i for i in f_names if i.find('fielding') > -1]
    assert len(gloves) == 2  # will be 18
    chad_csv = ['chadwick.csv']

    if fielding is True:
        to_check += gloves
    if chadwick is True:
        assert os.path.isfile(os.path.join(data_dir, 'chadwick.csv'))
        to_check += chad_csv

    now = time.time()
    for f in to_check:
        created = os.path.getmtime(os.path.join(data_dir, f))
        age = now - created
        if age > 3600 * 24 * expiry:
            print "Alert: " + f + " is more than {} day(s) old.".format(expiry)

    # check file size is > arbitrary value
    paths = [os.path.join(data_dir, i) for i in to_check]
    for path in paths:
        assert os.path.getsize(path) > 0
        if os.path.getsize(path) < 10000:
            print "Alert: " + path + " is very small."
    return  # f_names


def main(fielding=False, chadwick=False):
    """Grab data and write core files."""
    """Options for fielding data and bio data for rookies/master."""
    name_url_pairs = url_maker(fielding=fielding)
    # loop over tuples and get_dats
    for pair in name_url_pairs:
        get_data(pair[1], pair[0])
    # either do chadwick or not
    if chadwick is True:
        get_biographical()
    # Check if data is there, new and in range of len
    check_files(1, fielding=fielding, chadwick=chadwick)


def get_data(url, name):
    """Grab csv and html from bbref url and write to data folder."""
    """Will overwrite existing files.
    Runtime is about 30sec per page.
    -> Fielding data would take about 5mins.
    """
    # make the paths for the files
    # and the directory if needed
    # cwd = os.path.getcwd()
    stats_dir = os.path.join('', 'data', 'data' + year)
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
    br.click('input[type="checkbox"]')
    br.wait_load(10)
    # grab the html before changing to csv

    with open(html_path, 'w') as f:
        f.write(br.html.encode('utf-8'))

    print "HTML written."
    # convert table to csv on page. Will be contained in only <pre>
    # fielding query ('span[tip$="values"]')[2] # third of 4
    if url.find('fielding') == -1:
        br.runjs("""jQuery('span[tip$="values"]:last').click()""")
        br.wait(5)
    else:
        br.runjs("""jQuery('span[tip$="values"]:eq(2)').click()""")
        br.wait(5)

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
    except AttributeError:
        print "Spynner problem closing browser."
    # br.close()
    # print "060"
    return None


def get_biographical():
    """D/L biographical data from Chadwick Bureau."""
    url = 'https://raw.githubusercontent.com/chadwickbureau/'
    url += 'register/master/data/people.csv'
    print "Getting Chadwick Bureau data."
    print "This may take a minute."
    chad_path = os.path.join('', 'data', 'data' + year)
    browser = spynner.Browser()
    cb_csv = browser.download(url)
    with open(os.path.join(chad_path, 'chadwick.csv'), 'w') as cb:
        cb.write(cb_csv.encode('utf-8'))
    return


def do_stuff():
    """Test to make sure REPL freeze doesn't screw things."""
    h = os.getcwd()
    g = os.path.dirname(h)
    h = os.path.join(g, 'data', 'data' + year)
    if os.path.isdir(h):
        print 'isdir'
    else:
        os.mkdir(h)
        print "made new directory for {}".format(year)

    print g
    return h, g


# the jQuery/pyQuery finder for "Hide all non-qualifiers"
# quals = '''$('input[type="checkbox"]').click()'''
# the jQ for the download.
# dl_links = '''$('span[tip$="Excel"]')'''
# db_links[0] is teams [1] is players


# br.show()

target = """<span tip="Get a downloadable file suitable for Excel"
            class="tooltip"
            onclick="sr_download_data('players_standard_batting');
            try { ga('send','event','Tool','Action','Export'); }
            catch (err) {}">
            Export</span>"""

js = """sr_download_data('players_standard_batting');
        try { ga('send','event','Tool','Action','Export');
        } catch (err) {}"""
# print js
# br.runjs(js)
# br.wait_load()

# anchors = br.webframe.findAllElements('span[tip$="Excel"]')
# print "len of anchors: ",
# print len(anchors)
# print anchors[1].toPlainText()
# br.wk_click_element_link(anchors[1], timeout=10)
# br.click(exports[1], timeout=20)
# print "Noticed the click"

# d = pyquery.PyQuery(br.html)
# print str(d('span[tip$="Excel"]'[1]))
# print br.('span[tip$="Excel"]:last')
# br.click('span[tip$="Excel"]:last', wait_load=True)
# tar = """"""

# br.runjs("""jQuery('span[tip$="Excel"]:last').click()""")

# alt technique click to change to csv, grab data
# prior to converting the data we need the html links

'''
pyq_doc = pyquery.PyQuery(br.html)
# the comma separated data is in the only 'Pre' tags
csv = pyq_doc('pre').text()
# print "here"
# print len(csv)
# print type(csv)
# print csv[:200]

f = open('trial', 'w')
f.write(csv.encode('utf-8'))
f.close

# markup = br._get_html()
'''
# print os.getcwd()
