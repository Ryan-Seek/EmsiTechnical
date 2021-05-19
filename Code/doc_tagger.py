import json
import sqlite3
import pandas as pd#had to pip install pandas
from htmlstripper import Stripper



class DocTagger():
    source_file = "./Resources/data"
    create_table_statement = """ CREATE TABLE IF NOT EXISTS dataTags (
                    body text,
                    title text,
                    expired text,
                    posted text,
                    state text,
                    city text,
                    onet text,
                    soc5 text,
                    soc2 text
                    ); """

    def __int__(self):
        
        self.db_name = ""
        self.onet_map = None
        self.pc_relation=None
        self.conn = None
        self.html_tags_removed = None
        self.posts_active = None
        

    #database functions
    def create_db_connection(self):
        try:
            db_conn = sqlite3.connect(self.db_name)

        except sqlite3.Error as e:
            print(e)
            return False
        
        finally:
            return db_conn

    def create_table(self):
        try:
            cur = self.conn.cursor()
            cur.execute(self.create_table_statement)

        except sqlite3.Error as e:
            print(e)
    
    def insert_data(self, body, title, expired, posted, state, city, onet, soc5, soc2):
        try:
            row = (body, title, expired, posted, state, city, onet, soc5, soc2)
            sql = ''' INSERT INTO dataTags(body, title, expired, posted, state, city, onet, soc5, soc2) VALUES(?,?,?,?,?,?,?,?,?) '''
            cur = self.conn.cursor()
            cur.execute(sql, row)
            self.conn.commit()
        except sqlite3.Error as e:
            print(e)

    def print_summary(self):
        print('\n')
        print("* * * Document Tags Summary * * *")
        #Number of documents from which you successfully removed HTML tags.
        print("Number of HTML tags sucessfully removed: ", self.html_tags_removed)

        print('\n')
        #Count of documents for each soc2.
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(pd.read_sql_query("SELECT soc2, COUNT(soc2) FROM dataTags GROUP BY soc2", self.conn))

        print('\n')
        #Total number of postings that were active on February 1st, 2017. 2017-02-01
        print("Number of entries that were active on date February 1st, 2017:", self.posts_active)

    
    #debug functions to print whole table
    def get_tags(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM dataTags")
        print(cur.fetchall())

    #file functions
    def load_onet(self):
        onet_soc_map = None
        try:
            onet_soc_map = pd.read_csv("./Resources/map_onet_soc.csv")#read onet soc map into pandas dataframe
        except:
            print("Error opening map_onet_soc.csv")
        finally:
            return onet_soc_map

    def load_soc5_soc2(self):
        relation = None
        try:
            relation = pd.read_csv("./Resources/soc_hierarchy.csv")#read soc_hierarchy into pandas dataframe
        except:
            print("Error opening soc_hierarchy.csv")
        finally:
            return relation


#utility functions
    def strip_html(self, html):
        stripped = Stripper()
        stripped.feed(html)
        
        tags = self.html_tags_removed
        if(stripped != html):
            tags +=1

        return stripped.get_data(), tags

    def check_date(self, date_expired, date_posted):
        posts = self.posts_active 
        if (date_posted <='2017-02-01') & (date_expired >'2017-02-01'): #posted on or before February 1st, 2017, and expired after February 1st, 2017 thus active that day
             posts+=1
        
        return posts

    def onet_to_soc5(self, onet_val):
        soc5_val = None
        try:
            soc5_val = self.onet_map.loc[self.onet_map['onet'] == onet_val]['soc5'].values[0]
        except:
            return None
        finally:
            return soc5_val

    def soc5_to_soc2(self,soc5_val):
        spc2_val = None
        try:
            spc2_val = self.pc_relation.loc[self.pc_relation['child'] == soc5_val]['parent'].values[0]
        except:
            return None
        finally:
            return spc2_val


#main class function
    def process_data(self, name):
        self.db_name = name
        self.html_tags_removed = 0
        self.posts_active = 0

        #load onet sec5 map into pandas dataframe
        self.onet_map = self.load_onet()
        if self.onet_map.empty:
            print("Exiting program, issues opening map_onet_soc.csv")
            return

        #load parent child relationship json into pandas dataframe
        self.pc_relation = self.load_soc5_soc2()
        if self.pc_relation.empty:
            print("Exiting program, issues opening soc_hierarchy.csv")
            return

        #create database if does not exist, if it does then connect database 
        self.conn = self.create_db_connection()
        if self.conn  == False:
            print("Exiting program, issues connecting to database")
            return
        
        #create table in database only if the table does not exist, aka fresh database
        self.create_table()
        
        #open data file and stream lines to process
        f = open(self.source_file)
        line = f.readline()

        while line:#end processing when the stream is empty of data
            
            data = json.loads(line)
            
            #strip html from body
            body, self.html_tags_removed = self.strip_html(data['body'])

            #get soc5 from onet
            soc5 = self.onet_to_soc5(data['onet'])
            
            #get soc2 from soc5 and pc relation
            soc2 = self.soc5_to_soc2(soc5)

            #insert into table
            self.insert_data(body, data['title'], data['expired'], data['posted'], data['state'], data['city'], data['onet'], soc5, soc2)

            #checks date to update summary info
            self.posts_active = self.check_date(data['expired'], data['posted'])
            #get next line
            line = f.readline()
        
        self.print_summary()
        
        #close connection to database
        self.conn.close()
            

 #Main
if __name__ == "__main__":
    tagger = DocTagger()
    tagger.process_data("testDB")#database name 'testDB'
    