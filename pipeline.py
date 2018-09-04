#!/usr/bin/env python
import signal
import sys
import argparse

from lib import pipeline

def signal_handler(signal, frame):
    print('Ctrl-c detected. Pipeline is going down!')
    sys.exit(0)

def create_main_parser():
    parser = argparse.ArgumentParser(description='csv parser', prog='pipeline')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--source',
                        dest='source', help='csv file source')
    group.add_argument('--source-url',
                        dest='source', help='csv file source')
    parser.add_argument('--sink-user',
                        dest='user', help='db user', required=True)
    parser.add_argument('--sink-password',
                        dest='password', help='db password', required=True)
    parser.add_argument('--sink-host',
                        dest='host', help='db host', required=True)
    parser.add_argument('--sink-database',
                        dest='db', help='db name', required=True)
    parser.add_argument('--sink-table',
                        dest='table', help='db table', required=True)
    parser.add_argument('--sink-pk-idx',
                        dest='pk_idx', help='primary key field index',
                        type=int, default=0)
    parser.add_argument('--sink-pk-type',
                        dest='pk_type', help='primary key field type',
                        default='int')
    parser.add_argument('--debug',
                        dest='debug', help='debug flag',
                        default=False)
    return parser

def main():
    signal.signal(signal.SIGINT, signal_handler)
    parser = create_main_parser()
    args = parser.parse_args()

    dbconfig = {
        'host': args.host,
        'user': args.user,
        'password': args.password,
        'database': args.db
    }

    pipeline.process_cli(args.source,
                     args.table,
                     dbconfig,
                     pk_idx=args.pk_idx,
                     pk_type=args.pk_type,
                     debug=args.debug)

if __name__ == "__main__":
    main();
