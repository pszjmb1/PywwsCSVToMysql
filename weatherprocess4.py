#!/usr/bin/env python
'''
Shadow Data Parser parses data from Pywws formatted weather station data into the ShadowPress mysql DB.
    Copyright (C) 2012  Jesse Blum (JMB)

Parses a directory tree with Pywws weather data csv files into shadowpress.
In this version the database is added to indiscriminant of currently contained content and presumes the DB structure is in place.
Also this version hardcodes the tablenames, device instance and readingset info.

License: 

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
parser.add_argument('--s', help='supress warnings', required=False, action='store_true')
args = parser.parse_args()

#Connect to db
con = None
try:
	con = mdb.connect(args.dburl,  args.user, args.pwrd, args.db);
	with con:    
		cur = con.cursor()
                  
		# Walk the given root folder and add all data 
		hum_in=1
		temp_in=2
		hum_out=3
		temp_out=4
		abs_pressure=5
		wind_ave=6
		wind_gust=7
		wind_dir=8
		rain=9
		readingSetId=2
		horz_sp_deviceinstance_idhorz_sp_deviceinstance = 1
		horz_sp_readingset_info_horz_sp_readingset_info_id = 1;
		for root, dirs, files in os.walk(args.root_folder, topdown=True):
			dirs.sort()
			for name in sorted(files):
				print "Importing: "+os.path.join(root, name) 
				afilepath =os.path.join(root, name) 
				weatherReader = csv.reader(open(afilepath, 'rb'), delimiter=',')
				readingSetInsert = "INSERT INTO horz_sp_readingset (`readingset_id`, `timestamp`, `horz_sp_deviceinstance_idhorz_sp_deviceinstance`, `horz_sp_readingset_info_horz_sp_readingset_info_id`) VALUES "
				insertstringDec="INSERT INTO `horz_sp_reading`(`value_dec_8_2`,`horz_sp_readingset_readingset_id`, `horz_sp_reading_type_idhorz_sp_reading_type`) VALUES ("
				insertstringInt="INSERT INTO `horz_sp_reading`(`value_int`, `horz_sp_readingset_readingset_id`, `horz_sp_reading_type_idhorz_sp_reading_type`) VALUES ("
				for row in weatherReader:
					#Ensure that the CSV rows are legit
					row = [r or "NULL" for r in row]  
					#Add a readingset
					readingSetInsert = readingSetInsert + "(" + str(readingSetId) + ", '" + row.pop(0) + "'," + str(horz_sp_deviceinstance_idhorz_sp_deviceinstance) + "," + str(horz_sp_readingset_info_horz_sp_readingset_info_id) + "),"
					#Add readings
					# The following magic numbers are the db ids for the corresponding idhorz_sp_reading_type 
                    			insertstringInt=insertstringInt+row[hum_in]+", "+ str(readingSetId) + ",2),("
					insertstringInt=insertstringInt+row[hum_out]+", "+ str(readingSetId) + ",4),("
					insertstringInt=insertstringInt+row[wind_dir]+", "+ str(readingSetId) + ",7),("
					insertstringDec=insertstringDec+row[temp_in]+", "+ str(readingSetId) + ",3),("
					insertstringDec=insertstringDec+row[temp_out]+", "+ str(readingSetId) + ",1),("
					insertstringDec=insertstringDec+row[abs_pressure]+", "+ str(readingSetId) + ",5),("
					insertstringDec=insertstringDec+row[wind_ave]+", "+ str(readingSetId) + ",6),("
					insertstringDec=insertstringDec+row[wind_gust]+", "+ str(readingSetId) + ",8),("
					insertstringDec=insertstringDec+row[rain]+", "+ str(readingSetId) + ",9),("
					readingSetId = readingSetId + 1
				readingSetInsert=readingSetInsert[:-1]
				insertstringInt=insertstringInt[:-2]
				insertstringDec=insertstringDec[:-2]
				print readingSetInsert
				print insertstringInt
				print insertstringDec
				try:
					cur.execute(readingSetInsert)
					cur.execute(insertstringInt)
					cur.execute(insertstringDec)
				except mdb.Error, e:  
				   if(not args.s):
					print "Error %d: %s" % (e.args[0],e.args[1])			
	
except mdb.Error, e:
  
    print "Error %d: %s" % (e.args[0],e.args[1])				
    sys.exit(1)
    
finally:    
        
    if con:    
        con.close()
    print 'Done.'
