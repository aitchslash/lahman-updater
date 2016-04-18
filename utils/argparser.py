# /usr/bin/env python
"""Test using argparse."""

import argparse
import sys


def process_args(args):
    """Test."""
    # year = time.time yadda yadda
    # check lower bound for baseball data
    # throw value in -y flag
    # need file name too
    parser = argparse.ArgumentParser(description="updates the lahman db")
    parser.add_argument('-i',
                        '--ignore',
                        action='store_true',
                        help='Force get of new data ingnoring expiry')

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
                        default=2016,
                        help='Year of data to get')

    parser.add_argument('-d',
                        '--dbloginfile',
                        default='data/db_details.txt',
                        help='path to file with db login details')
    parser.add_argument('-x',
                        '--expanded',
                        action='store_false',
                        default=True,
                        help='Expand fields in db (e.g. FIP, OPS, MLBamID)')
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
