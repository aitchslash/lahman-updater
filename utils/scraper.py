"""Grab bbref data with spynner.

Get csv and html data, store in file
and move to correct location.

Depending on your d/l speed and the speed of baseball-reference's
server you may wish to change the wait_load values.  More time is
slower but you're more likely to get the data.
"""

import spynner
import os
import time
import datetime
import urllib2
import logging
from contextlib import contextmanager
from bs4 import BeautifulSoup
# import pyquery

# base url for testing, need to write script
# to generate url's for other years
# url_start = 'http://www.baseball-reference.com/leagues/MLB/'
# url_end = '-standard-batting.shtml'

year = '2016'  # just here so pep-8 stops complaining

# example of changing browser, not in use
# useragent = "Mozilla/5.0 (Windows NT 6.1; rv:7.0.1)
# Gecko/20100101 Firefox/7.0.1"

module_log = logging.getLogger('main.scraper')


@contextmanager
def ignored(*exceptions):
    """Suppress errors."""
    try:
        yield
    except exceptions:
        pass


# closing with context
@contextmanager
def closing(thing):
    """Context manager for ensuring spynner browser close."""
    try:
        yield thing
    finally:
        with ignored(BaseException, AttributeError, Exception):
            thing.close()


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


def check_files(year=year, expiry=1, fielding=False):  # , chadwick=False):
    """Verify files in data are new and of expected sizes."""
    """Set expiry to ensure that files are only 'expiry' days old."""
    """Intended to be run as import from main dir.  Paths are affected."""
    past_due, exists = False, True
    data_dir = os.path.join('', 'data', 'data' + str(year))
    # loggin.debug(data_dir)
    if os.path.isdir(data_dir) is False:
        module_log.warning("Data dir for " + str(year) + " is missing.")
        return True, False  # past_due, exists  # nb, uglyish
    f_names = os.listdir(data_dir)
    arms_bats = [i for i in f_names if (i.find('arms') + i.find('bats')) != -2]
    to_check = [] + arms_bats
    assert len(arms_bats) == 6

    # chad_csv = ['chadwick.csv']

    if fielding is True:
        gloves = [i for i in f_names if i.find('fielding') > -1]
        # loggin.debug(len(gloves))
        assert len(gloves) == 18  # will be 18
        to_check += gloves
    '''
    if chadwick is True:
        assert os.path.isfile(os.path.join(data_dir, 'chadwick.csv'))
        to_check += chad_csv'''

    now = time.time()
    for f in to_check:
        # nb, this if block is likely redundant
        if os.path.isfile(os.path.join(data_dir, f)) is False:
            exists = False
            module_log.warning(f + " not found.")
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
            module_log.warning("Alert: " + path + " is very small.")
    if past_due:
        module_log.warning("Alert: files are more than {} day(s) old.".format(expiry))
    return past_due, exists


def get_all_data(year=year, expiry=1, fielding=False, chadwick=False):
    """Grab all data and write core files."""
    """Options for fielding data and bio data for rookies/master."""
    name_url_pairs = url_maker(year=year, fielding=fielding)
    # if debugging warn about the webviews
    if module_log.isEnabledFor(logging.DEBUG):
        print "ALERT: Spynner windows should open."
        print "ALERT: This throws more AttributeError(s)."
        print "ALERT: No need to worry. They're uncaught but it all works."
    # loop over tuples and get_dats
    for pair in name_url_pairs:
        get_data(pair[1], pair[0], year)
    # either do chadwick or not
    if chadwick is True:
        get_biographical()
    # Check if data is there, new and in range of len
    past_due, exists = check_files(year, expiry, fielding=fielding)  # , chadwick=chadwick)
    if past_due is False and exists is True:
        module_log.info("Files now up to date.")
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
        module_log.info("Need to make directory.")
        os.mkdir(stats_dir)
    csv_path = os.path.join(stats_dir, name + '.csv')
    html_path = os.path.join(stats_dir, name + '.shtml')
    # start up spynner
    # br = spynner.Browser()  # old error-throwing, yet funcitonal
    # br = spynner.browser.Browser(embed_jquery=True,
    #                             debug_level=0,)

    # if debugging open up the webviews
    if module_log.isEnabledFor(logging.DEBUG):
        webview = True
    else:
        webview = False

    # try a context manager
    # with closing(spynner.Browser(debug_level=spynner.ERROR, debug_stream=module_log.DEBUG)) as br:
    with closing(spynner.Browser()) as br:
        # url = url_start + year + url_end

        # the page takes a while to load, 10 was too little
        br.load(url, load_timeout=15)

        if webview:
            br.create_webview()
            # may want to get rid of show() - nice for debugging.
            br.show()

        module_log.info("Processing " + name)
        br.load_jquery(True)
        # unhide non-qualifiers
        try:
            br.click('input[type="checkbox"]')
            br.wait_load(10)
        except spynner.SpynnerTimeout:
            module_log.error("timed out.")
            # raise spynner.SpynnerTimeout

        # grab the html before changing to csv

        with open(html_path, 'w') as f:
            f.write(br.html.encode('utf-8'))

        module_log.info("HTML written.")
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

        try:
            soup = BeautifulSoup(br.html, 'html.parser')
        except:
            module_log.debug("problems making soup.")
            pass
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

        module_log.info("CSV written.")
        # f(x) freezing in REPL after successful execution
        # tried the two lines below; didn't help
        # br.destroy_webview()
        # print "050"
        if webview:
            try:
                br.destroy_webview()
                module_log.debug("Before close.")
                # br.close()
                # print "After close."
            except Exception as error:
                print "Spynner problem closing webview. {}".format(error)
                pass

    # br.close()
    # module_log.DEBUG("After close.")
    return None


def check_chadwick():
    """Verify that there is new data in the current chadwick repository."""
    csv_path = 'data/people.csv'
    fresh = True
    now = time.time()
    one_day = 60 * 60 * 24
    six_days = one_day * 6
    # check there is a file
    file_exists = os.path.exists(csv_path)
    if file_exists:
        file_size = os.path.getsize(csv_path)
        module_log.debug(file_size)
        if file_size < 35000000:
            module_log.warning('Current version of Chadwick too small')
            fresh = False
        elif file_size < 37000000:
            module_log.debug("Right size.")
        else:
            module_log.warning("ALERT: Chadwick is very big.")
    else:
        module_log.warning("File missing from path.")
        fresh = False
        return fresh

    # check age of current file
    last_modified = os.path.getmtime(csv_path)
    # ack, not working, @fix_csv likey mucks up creation date

    # check age of chadwick on github
    url = "https://github.com/chadwickbureau/register"
    page = urllib2.urlopen(url).read()
    soup = BeautifulSoup(page, 'html.parser')
    #   document.getElementsByClassName('age')
    ages_of_files = soup.find_all('td', {'class': "age"})
    #   assert that the page is as expected i.e. len results == 2
    assert len(ages_of_files) == 2
    date_string = ages_of_files[0].text.encode('ascii').strip('\n')
    module_log.debug(date_string)
    struct_time = datetime.datetime.strptime(date_string, '%B %d, %Y')
    git_created_time = time.mktime(struct_time.timetuple()) + one_day
    # if chadwick isn't fresh return False
    # unlikely, using modified rather than created, but worthwhile
    if git_created_time > last_modified:
        module_log.debug(now - git_created_time)
        module_log.debug(now - last_modified)
        module_log.info("Chadwick more recent than current file.")
        fresh = False
    # chadwick says they update every 5 days or so
    elif now - git_created_time > six_days:
        module_log.info("Chadwick less than 5 days old")
        fresh = False
    return fresh


def get_biographical():
    """D/L biographical data from Chadwick Bureau."""
    url = 'https://raw.githubusercontent.com/chadwickbureau/'
    url += 'register/master/data/people.csv'
    print "Getting Chadwick Bureau data."
    print "~35MB file. This may take a minute."
    chad_path = os.path.join('', 'data')
    browser = spynner.Browser()
    cb_csv = browser.download(url)
    with open(os.path.join(chad_path, 'people.csv'), 'w') as cb:
        cb.write(cb_csv.encode('utf-8'))
    return
