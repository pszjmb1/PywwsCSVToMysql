#!/usr/bin/env python
'''Parses a directory tree with Pywws weather data csv files into mysql.
In this simple version the database is added to indiscriminant of currently contained content
'''

import os
import sys
import csv
import time
import argparse
import MySQLdb as mdb

#Command-line arguments
parser = argparse.ArgumentParser(description='Parse csv weather data files in a given folder into mysql.')
parser.add_argument('root_folder', help='The folder path to the csv files.')
parser.add_argument('dburl', default='localhost', help='URL string to the Database')
parser.add_argument('user', help='Database user')
parser.add_argument('pwrd', help='Database user pwrd')
parser.add_argument('db', help='Database to use')
args = parser.parse_args()

#Connect to db
con = None
try:
	con = mdb.connect(args.dburl,  args.user, args.pwrd, args.db);
	with con:    
		cur = con.cursor()
		cur.execute("CREATE TABLE IF NOT EXISTS \
		        data_weather(idx datetime PRIMARY KEY, delay int(4), hum_in int(2), temp_in decimal(4,1), hum_out int(2), temp_out decimal(4,1), abs_pressure decimal(6,1), wind_ave decimal(5,1), wind_gust decimal(5,1), wind_dir int(2), rain decimal(6,1), status int(1) )")
                  
		'''# Walk the given root folder and add all data 
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
						print rrdtool.error()'''



except mdb.Error, e:
  
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)
    
finally:    
        
    if con:    
        con.close()
