"""Goof around with spynner.

runjs directly
get rid of jupyter - user agent
"""

import spynner
import os
from bs4 import BeautifulSoup
import pyquery

url = 'http://www.baseball-reference.com/leagues/MLB/2016-standard-batting.shtml'
# useragent = "Mozilla/5.0 (Windows NT 6.1; rv:7.0.1) Gecko/20100101 Firefox/7.0.1"
br = spynner.Browser()
# br.wait_load(timeout=30)
br.load(url, load_timeout=15)

br.create_webview()
br.show()
quals = '''$('input[type="checkbox"]').click()'''
dl_links = '''$('span[tip$="Excel"]')'''
# db_links[0] is teams [1] is players

br.load_jquery(True)
br.click('input[type="checkbox"]')
br.wait_load()
# br.show()

target = """<span tip="Get a downloadable file suitable for Excel"
            class="tooltip" onclick="sr_download_data('players_standard_batting');
            try { ga('send','event','Tool','Action','Export'); } catch (err) {}">
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

f = open('stats_html', 'w')
f.write(br.html.encode('utf-8'))
f.close


# converts to csv on page contained in <pre>
br.runjs("""jQuery('span[tip$="values"]:last').click()""")

br.wait(10)
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

soup = BeautifulSoup(br.html, 'html.parser')
pre_section = soup.find('pre').get_text()
print type(pre_section)
print len(pre_section)
with open('bsTest', 'w') as p:
    p.write(pre_section.encode('utf-8'))


# markup = br._get_html()

print os.getcwd()
