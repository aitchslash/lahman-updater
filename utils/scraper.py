"""Grab bbref data with spynner.

Get csv and html data, store in file
and move to correct location.
"""

import spynner
import os
from bs4 import BeautifulSoup
# import pyquery

# base url for testing, need to write script
# to generate url's for other years
url_start = 'http://www.baseball-reference.com/leagues/MLB/'
url_end = '-standard-batting.shtml'
year = '2016'

# example of changing browser, not in use
# useragent = "Mozilla/5.0 (Windows NT 6.1; rv:7.0.1)
# Gecko/20100101 Firefox/7.0.1"


def url_maker():
    """Make url."""
    pass

url = url_start + year + url_end


def get_data(url, name):
    """Grab csv and html from bbref url and write to data folder."""
    """Will overwrite existing files.
    Runtime is about 30sec per page.
    -> Fielding data would take about 5mins.
    """
    # make the paths for the files
    # and the directory if needed
    # cwd = os.path.getcwd()
    stats_dir = os.path.join(os.pardir, 'data', 'data' + year)
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
    # br.create_webview()
    # may want to get rid of show() - nice for debugging.
    br.show()
    br.load_jquery(True)
    # unhide non-qualifiers
    br.click('input[type="checkbox"]')
    br.wait_load()
    # grab the html before changing to csv

    with open(html_path, 'w') as f:
        f.write(br.html.encode('utf-8'))

    print "HTML written."
    # convert table to csv on page. Will be contained in only <pre>
    br.runjs("""jQuery('span[tip$="values"]:last').click()""")
    br.wait(10)

    soup = BeautifulSoup(br.html, 'html.parser')
    pre_section = soup.find('pre').get_text()
    with open(csv_path, 'w') as p:
        p.write(pre_section.encode('utf-8'))

    print "CSV written."
    # f(x) freezing in REPL after successful execution
    # tried the two lines below; didn't help
    # br.destroy_webview()
    br.close()
    # print "Please don't freeze."
    return None


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


def main():
    """Tester."""
    get_data()
    cwd, dirstr = do_stuff()
    return cwd, dirstr

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
print os.getcwd()
