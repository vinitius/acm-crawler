import MySQLdb
import gc

host="localhost" 
user="root" 
passwd="root" 
database="ACM"



class DatabaseHelper:	
	def __init__(self):
		return

	def closeLocalConnection(self):
		self.db.close()	
		gc.collect()
					
	def connectToLocalDatabase(self):
		try:
			self.db = MySQLdb.connect(host=host, 
                     user=user, 
                      passwd=passwd, 
                      db=database,
                      charset='utf8',
                      use_unicode=True)
			return self.db
		except:
			return	

	def insertPublication(self,id,title,authors,date):
		cur = self.db.cursor() 	
		cur.execute("SELECT * FROM PUBLICATION WHERE id_publication=%s",[id])			
		if not cur.fetchone():					
			cur.execute("INSERT INTO PUBLICATION(id_publication,title,authors,date) VALUES(%s,%s,%s,%s)",[id,title,authors,date])
			self.db.commit();
		cur.close()

	def insertAuthor(self,id,name):
		cur = self.db.cursor() 	
		cur.execute("SELECT * FROM AUTHOR WHERE id_author="+id)
		if not cur.fetchone():
			cur.execute("INSERT INTO AUTHOR(id_author,name) VALUES("+id+",%s)",[name])
			self.db.commit();
		cur.close()	

	def insertConference(self,id,name,abbrv):
		cur = self.db.cursor() 	
		cur.execute("SELECT * FROM CONFERENCE WHERE id_conference=%s",[id])
		if not cur.fetchone():
			cur.execute("INSERT INTO CONFERENCE VALUES(%s,%s,%s)",[id,name,abbrv])
			self.db.commit();
		cur.close()	

	def addAuthorToPublication(self,id_publication,id_author):
		cur = self.db.cursor() 		
		cur.execute("SELECT * FROM PUBLICATION_HAS_AUTHORS WHERE id_publication=%s AND id_author="+id_author,[id_publication])
		if not cur.fetchone():	
			cur.execute("INSERT INTO PUBLICATION_HAS_AUTHORS VALUES(%s,"+id_author+")",[id_publication])
			self.db.commit();
		cur.close()		

	def addConferenceToPublication(self,id_publication,id_conference):
		cur = self.db.cursor() 		
		cur.execute("SELECT * FROM PUBLICATION_HAS_CONFERENCE WHERE id_publication=%s AND id_conference=%s",[id_publication,id_conference])
		if not cur.fetchone():		
			cur.execute("INSERT INTO PUBLICATION_HAS_CONFERENCE VALUES(%s,%s)",[id_publication,id_conference])
			self.db.commit();
		cur.close()			

	def addReferenceToPublication(self,id_publication,id_reference):
		cur = self.db.cursor() 			
		cur.execute("SELECT * FROM PUBLICATION_HAS_REFERENCES WHERE id_publication=%s AND id_reference=%s",[id_publication,id_reference])
		if not cur.fetchone():	
			cur.execute("INSERT INTO PUBLICATION_HAS_REFERENCES VALUES(%s,%s)",[id_publication,id_reference])
		self.db.commit();
		cur.close()			

	def addToQueue(self,id):
		cur = self.db.cursor() 	
		cur.execute("INSERT INTO QUEUE VALUES(%s)",[id])
		self.db.commit();
		cur.close()

	def removeFromQueue(self,id):
		cur = self.db.cursor() 	
		cur.execute("DELETE FROM QUEUE WHERE id_publication=%s",[id])
		self.db.commit();
		cur.close()

	def findInQueue(self,id):
		cur = self.db.cursor() 			
		cur.execute("SELECT id_publication FROM QUEUE WHERE id_publication=%s",[id])
		found = cur.fetchone()
		cur.close()
		return found

	def findPublication(self,id):
		cur = self.db.cursor() 			
		cur.execute("SELECT id_publication FROM PUBLICATION WHERE id_publication=%s and full_collected=1",[id])
		found = cur.fetchone()
		cur.close()
		return found	


	def setPublicationFullCollected(self,id):
		cur = self.db.cursor() 			
		cur.execute("UPDATE PUBLICATION SET full_collected=1 WHERE id_publication=%s and full_collected=0",[id])
		self.db.commit()
		cur.close()		

	def getQueue(self,offset):					
		cur = self.db.cursor() 			
		cur.execute("SELECT id_publication FROM QUEUE LIMIT 0,"+str(offset))
		queue = cur.fetchall()
		cur.close()
		return queue	

	def clearDatabase(self):					
		cur = self.db.cursor() 			
		cur.execute("DELETE FROM PUBLICATION_HAS_AUTHORS")
		cur.execute("DELETE FROM PUBLICATION_HAS_REFERENCES")
		cur.execute("DELETE FROM PUBLICATION_HAS_CONFERENCE")
		cur.execute("DELETE FROM PUBLICATION")
		cur.execute("DELETE FROM AUTHOR")
		cur.execute("DELETE FROM CONFERENCE")
		cur.execute("DELETE FROM QUEUE")		
		self.db.commit()
		cur.close()
			


			