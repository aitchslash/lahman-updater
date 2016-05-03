# /usr/bin/env python
"""Test using argparse."""

import argparse
import sys
import time
# import logging


def set_default_season():
    """Return current year (cy) or cy - 1 if off-season."""
    cy = time.strftime("%Y-%m")
    year, month = cy.split("-")
    if int(month) <= 3:
        year = str(int(year) - 1)
    return year


def process_args(args):
    """Test."""
    year = set_default_season()
    # check lower bound for baseball data
    # throw value in -y flag
    # need file name too
    parser = argparse.ArgumentParser(description="updates the lahman db")
    parser.add_argument('--setup',
                        action='store_true',
                        help='updates 2014 db upto and including current season')

    parser.add_argument('--reset',
                        action='store_true',
                        help='reset db to 2014 and delete expanded fields')

    parser.add_argument('-v',
                        '--verbose',
                        action='count',
                        default=0,
                        help="increase verbosity: 0 => warnings, 1 => info, 2 => debug")

    parser.add_argument('-i',
                        '--ignore',
                        action='store_true',
                        help='Force get of new data ingnoring expiry')

    parser.add_argument('-x',
                        '--expand',
                        action='store_true',
                        help='boolean, update db with expanded stats for -y year')

    parser.add_argument('-e',
                        '--expiry',
                        type=int,
                        default=1,
                        help='data age allowable in days, default 1')

    parser.add_argument('-y',
                        '--year',
                        type=int,
                        # range works but makes help ugly.
                        # choices=xrange(1900, 2017),  # nb, year + 1 goes here
                        default=int(year),
                        help='Year of data to get')

    parser.add_argument('-d',
                        '--dbloginfile',
                        default='data/db_details.txt',
                        help='path to file with db login details')

    parser.add_argument('-s',
                        '--strict',
                        action='store_false',
                        default=False,
                        help='Set flag to not use expanded db stats (e.g. FIP, OPS, MLBamID)')
    parser.add_argument('-f',
                        '--fielding',
                        action='store_true',
                        default=False,
                        help='update fielding (NB time consuming)'),
    parser.add_argument('-c',
                        '--chadwick',
                        action='store_true',
                        help='Download ~35Mb csv from chadwick-bureau')
    option = parser.parse_args(args)
    return vars(option)


def main():
    """Tester."""
    options = process_args(sys.argv[1:])
    for option in options:
        print str(option) + ': ' + str(options[option])


if __name__ == '__main__':
    main()
    '''
    parser = argparse.ArgumentParser(description="Say hello")
    parser.add_argument('name', help='your name, enter it')
    args = parser.parse_args()

    main(args.name)
    '''
