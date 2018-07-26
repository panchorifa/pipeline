#! /usr/bin/env python
# coding=utf-8

"""
pipeline

Parses csv data and stores it in mysql table

Uses ....
"""

import argparse

def create_main_parser():
    parser = argparse.ArgumentParser(description='csv parser',prog='pipeline')
    parser.add_argument('--source-url', dest='source', help='csv file source')
    parser.add_argument('--sink-user', dest='user', help='db user')
    parser.add_argument('--sink-password', dest='pass', help='db pass')
    parser.add_argument('--sink-host', dest='host', help='db host')
    parser.add_argument('--sink-database', dest='db', help='db name')
    parser.add_argument('--sink-table', dest='table', help='db table')
    return parser

def main():
    """
    Parses csv
    :param x: some description
    :return: Versions of Python
    """
    parser = create_main_parser()
    args = parser.parse_args()

    print args

if __name__ == "__main__":
    main()


# ./pipeline.py
# --source https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2018-01-07.csv
# --sink-user root
# --sink-password password
# --sink-host mysql
# --sink-database external
# --sink-table npi
