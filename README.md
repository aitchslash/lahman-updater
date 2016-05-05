# lahman-updater
Update the 2014 Lahman database to most recent data, including in-season.

Working but still in progress.

Uses data scrapes from baseball-reference and downloads
from Chadwick Bureau.

Can also be used to add expanded stats to prior years.

Edit data/db_details.txt with your database access information.

Flags:
--setup             initial update of 2014 db to current data
--reset             reset db to 2014, removed expanded data if present
-v, --verbose       increase verbosity. default warnings only. -v add info, -vv add debug
-x, --expand        enters expanded stats into db for year given
-s, --strict        maintains schema of 2014 db
-y, --year          year to be updated/expanded, defaults to current season
-i, --ignore        force update
-f, --fielding      include fielding data, scrape is time consuming
-e, --expiry        set allowable age of data in days, default 1
-c, --chadwick      download new chadwick data, ~35Mb
-d, --dbloginfile   path to db login details
