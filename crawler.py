import os
import gc

from databaseHelper import DatabaseHelper


offset= 10
breakIntervalInSeconds = 5

dbHelper = DatabaseHelper()


if dbHelper.connectToLocalDatabase() :

	print "\n\nstarting..."

	publications = dbHelper.getQueue(offset)	
	for publication in publications:						
		if not dbHelper.findPublication(str(publication[0])):
			print "waiting "+str(breakIntervalInSeconds)+" seconds to collect..."
			os.system("sleep "+str(breakIntervalInSeconds))	
			print "collecting "+str(publication[0])
			os.system("python parser.py "+str(publication[0]))
		else:
			dbHelper.removeFromQueue(str(publication[0]))	


	if not publications:	
		print "Empty queue. Adding first publication to collect..."
		dbHelper.addToQueue('2000824.2000828') #first to colllect	

	dbHelper.closeLocalConnection()
	gc.collect()
	os.system("python crawler.py")
	
else:
	print "not able to connect to local database. Check 'databaseHelper.py'"	

	







