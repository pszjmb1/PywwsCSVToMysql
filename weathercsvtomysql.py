#!/usr/bin/env python
'''Parses a directory tree with Pywws weather data csv files into mysql'''

import os
import sys
import csv
import time
import argparse

parser = argparse.ArgumentParser(description='Parse csv weather data files in a given folder into mysql.')
parser.add_argument('root_folder', help='The folder path to the csv files.')
parser.add_argument('dburl', default='localhost', help='URL string to the Database')
parser.add_argument('user', help='Database user')
parser.add_argument('pwrd', help='Database user pwrd')
parser.add_argument('db', help='Database to use')
args = parser.parse_args()

for root, dirs, files in os.walk(args.root_folder, topdown=True):
	dirs.sort()
	for name in sorted(files):
		print os.path.join(root, name) 
		afilepath =os.path.join(root, name) 
		weatherReader = csv.reader(open(afilepath, 'rb'), delimiter=',')
		for row in weatherReader:
			row[0]=str(int(time.mktime( time.strptime(row[0], "%Y-%m-%d %H:%M:%S") ) ) )
			row.pop(1) 			# get rid of the delay value
			row.pop(10)			# get rid of the status value
			rowData = ':'.join(row)
			ret = rrdtool.update(rrdpath,rowData);
			if ret:
				print rrdtool.error()
print('Weatherprocess2 completed.')
