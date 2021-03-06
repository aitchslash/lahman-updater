lahman-updater
==============

Update the 2014 Lahman database to most recent data, including
in-season.

Uses data scrapes from baseball-reference and downloads from Chadwick
Bureau.

Can also be used to add expanded stats to prior years.

Working but still in progress.

Notes:
----------------------

-  written for python 2.7.11
-  requires spynner, beautifulsoup4 and pymysql
-  Spynner is tough to install.  easy\_install spynner - spynner has no pip
-  Edit data/db\_details.txt with your database access information.
-  Run with –setup and set –strict if you prefer the 2014 Lahman schema.

ISSUES:
~~~~~~

-  spynner dependencies (PyQt and SIP) are problematic to install in virtualenvs
-  Recommended to not use virtual env or to manually install packages.
-  Potential (untested) vitualenv fix here: http://stackoverflow.com/questions/22640640/how-to-install-pyqt4-on-windows-using-pip
-  Spynner throws errors that I've been unable to catch. This does not seem to affect performance.

Flags:
~~~~~~

+----------+-----------------------------------------------------+-------------------------------------------------------------------------+
| -flag    | –flag                                               | description                                                             |
+==========+=====================================================+=========================================================================+
|          | –setup                                              | initial update of 2014 db to current data                               |
+----------+-----------------------------------------------------+-------------------------------------------------------------------------+
|          | –reset                                              | reset db to 2014, remove expanded data if present                       |
+----------+-----------------------------------------------------+-------------------------------------------------------------------------+
| -v       | –verbose                                            | increase verbosity. default warnings only. -v add info, -vv add debug   |
+----------+-----------------------------------------------------+-------------------------------------------------------------------------+
| -x       | –expand                                             | enters expanded stats into db for year given                            |
+----------+-----------------------------------------------------+-------------------------------------------------------------------------+
| -s       | –strict                                             | maintains schema of 2014 db                                             |
+----------+-----------------------------------------------------+-------------------------------------------------------------------------+
| -y       | –year                                               | year to be updated/expanded, defaults to current season                 |
+----------+-----------------------------------------------------+-------------------------------------------------------------------------+
| -i       | –ignore                                             | force update                                                            |
+----------+-----------------------------------------------------+-------------------------------------------------------------------------+
| -f       | –fielding                                           | include fielding data, scrape is time consuming                         |
+----------+-----------------------------------------------------+-------------------------------------------------------------------------+
| -e       | –expiry                                             | set allowable age of data in days, default 1                            |
+----------+-----------------------------------------------------+-------------------------------------------------------------------------+
| -c       | –chadwick                                           | download new chadwick data, ~35Mb                                       |
+----------+-----------------------------------------------------+-------------------------------------------------------------------------+
| -d       | –dbloginfile                                        | path to db login details                                                |
+----------+-----------------------------------------------------+-------------------------------------------------------------------------+