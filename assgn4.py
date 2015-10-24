# Assignment 4 - Introduction to Amazon Web Services (and Databases)

# Name : Shweta Pathak
# UTA ID : 1001154572
# Net Id : ssp4572

# import statements

import boto
import csv
import time
import sys
import MySQLdb
import urllib2
import memcache
import hashlib
from boto.s3.key import Key
from boto.s3.connection import S3Connection
from boto.s3.connection import Location



# Upload the file to amazon S3
def put():

    # Access keys 
    AWS_ACCESS_KEY_ID= your aws access key
    AWS_SECRET_ACCESS_KEY=your secret access key

    # Establish connection with Amazon S3
    conn = S3Connection(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,validate_certs=False,is_secure=False)

    bucket_name = conn.create_bucket('shwetabucket91')
    k = Key(bucket_name)
    k.key = raw_input("Enter the file name to upload to S3: ")
    start_time = time.clock()
    k.set_contents_from_filename(k.key)
    end_time = time.clock()
    total_time = end_time-start_time
    print(total_time) 
      
# Inserting data into the Relational database service 
def insert_data():

    db = MySQLdb.connect(host= "shweta.ckcwra3d81nf.us-west-2.rds.amazonaws.com",user="root",passwd="root1234",db="earthquakes_database")
    # Access keys 
    AWS_ACCESS_KEY_ID='AKIAJGQE6BZY4X7LYAYA'
    AWS_SECRET_ACCESS_KEY='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'

    # Establish connection with Amazon S3
    conn = S3Connection(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,validate_certs=False,is_secure=False)

    bucket_name = conn.get_bucket('shwetabucket91')
    k = Key(bucket_name)

    url = 'https://s3.amazonaws.com/shwetabucket91/Inpatient_data.csv'
    response = urllib2.urlopen(url)
    csv_data = csv.reader(response)
    cursor = db.cursor()
    cursor.execute("drop table inpatient_data")
    cursor.execute("create table inpatient_data(DRG_Definition varchar(255),Provider_Id varchar(255),Provider_Name varchar(255),Address varchar(255),City varchar(255),State varchar(255),Zip varchar(255),Region varchar(255),Total_discharge varchar(255),Average_Covered_Charges varchar(255),Average_Total_Payments varchar(255),Average_Medicare_Payments varchar(255))")

    start_time = time.clock()
    count = 0
    for row in csv_data:
        count += 1
        if count <> 0:
            cursor.execute("INSERT INTO inpatient_data(DRG_Definition,Provider_Id,Provider_Name,Address,City,State,Zip,Region,Total_discharge,Average_Covered_Charges,Average_Total_Payments,Average_Medicare_Payments) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row)
            print count
            db.commit()
    end_time = time.clock()
    total_time = end_time - start_time
    print ("Total time taken to insert data into RDS : ")
    print total_time
    db.commit()

    
# Listing all the buckets available
def list_objects():
    # Access keys 
    AWS_ACCESS_KEY_ID='AKIAJGQE6BZY4X7LYAYA'
    AWS_SECRET_ACCESS_KEY='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'

    # Establish connection with Amazon S3
    conn = S3Connection(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,validate_certs=False,is_secure=False)

    bucket_name = conn.get_bucket('shwetabucket91')
    k = Key(bucket_name)

    bucket_list = conn.get_all_buckets()
    print(len(bucket_list))
    for rs in bucket_list:
        print rs.name

# get all contents from the file
def list_files():
        
    # Access keys 
    AWS_ACCESS_KEY_ID='AKIAJGQE6BZY4X7LYAYA'
    AWS_SECRET_ACCESS_KEY='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'

    # Establish connection with Amazon S3
    conn = S3Connection(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,validate_certs=False,is_secure=False)

    bucket_name = conn.get_bucket('shwetabucket91')
    k = Key(bucket_name)

    for key in bucket_name.list():
        print "{name}\t{size}\t{modified}".format(
                name = key.name,
                size = key.size,
                modified = key.last_modified,
                )

# Perform 1 , 5 and 20 thousand random queries

def queries():
   
    # Establish connection with MySQLdb
    db = MySQLdb.connect(host= "shweta.ckcwra3d81nf.us-west-2.rds.amazonaws.com",user="root",passwd="root1234",db="earthquakes_database")

    cursor = db.cursor()
    query = "select Provider_Id from inpatient_data order by "
    print " \nOne thousand random queries :"
    start_time = time.clock()
    for row in range(1,1001):
            cursor.execute(query)
            elapsed = (time.clock()-start_time)
            
    
    print "time taken to load", elapsed
    
    query = "select Provider_Id from inpatient_data order by rand()"
    print " \nFive thousand random queries :"
    start_time1 = time.clock()
    for tuple in range(1,5001):
            cursor.execute(query)
            elapsed1 = (time.clock()-start_time1)
            
    
    print "time taken to load", elapsed1

    query = "select Provider_Id from inpatient_data order by rand()"
    print " \nTwenty thousand random queries :"
    start_time2 = time.clock()
    for record in range(1,20001):
            cursor.execute(query)
            elapsed2 = (time.clock()-start_time1)
            
    
    print "time taken to load", elapsed2
    
    db.commit()
    print 'done'
    
def query_tuples():
    
    # Establish connection with MySQLdb
    db = MySQLdb.connect(host= "shweta.ckcwra3d81nf.us-west-2.rds.amazonaws.com",user="root",passwd="root1234",db="earthquakes_database")

    cursor = db.cursor()
    start_time = time.clock()
    print "Random queries on retrieved 200 to 800 tuples"
    cursor.execute("select Provider_Id from inpatient_data limit 199,600")
    fetch_data = cursor.fetchall()
   
    for row in fetch_data:
        for i in range(1,1001):
            cursor.execute("select Provider_Name from inpatient_data where Provider_Id = %s order by rand()", row)
            elapsed = (time.clock()-start_time)
       
    print "time taken to load", elapsed
    
def mem_query():
    
    # Establish connection with MySQLdb
    db = MySQLdb.connect(host= "shweta.ckcwra3d81nf.us-west-2.rds.amazonaws.com",user="root",passwd="root1234",db="earthquakes_database")

    memClient = memcache.Client(['shweta.rviwnp.0001.usw2.cache.amazonaws.com:11211'],debug=0)
    query = "select Provider_Id from inpatient_data order by rand()"
    hash_key = hashlib.md5()
    hash_key.update(query)
    key = hash_key.hexdigest()
    print key
   
    cursor = db.cursor()
    start_time = time.clock()
    if memClient.get(key): # If data already exists
        print " Got data"
        
    else:
        cursor.execute('select Provider_Id from inpatient_data order by rand()')
        rows = cursor.fetchall()
        memClient.set(key,rows)
        print "Not found"
    
    end_time = time.clock()
    total_time = end_time - start_time
    print " Total time taken by memcache :"
    print(total_time)
            



def main():
 
  options_toselect = {1: put, 2: insert_data, 3:list_objects, 4:list_files, 5: queries, 6:query_tuples,7: mem_query}
  while(True):
     
      print "\n1. Upload file on Amazon Cloud S3. \n"
      print "2. Insert data into Amazon Relational Data Service. \n"
      print "3. List of buckets on Amazon S3. \n"
      print "4. List the files in a bucket. \n"
      print "5. Time taken to execute 1 thousand , 5 thousand and 20 thousand Random queries.\n"
      print "6. Random Query for 200 to 800 tuples. \n"
      print "7. Elastic cache queries."
      print "8. Exit \n"
     
      option = raw_input("Select one option : ")
      if option =="1":
          options_toselect[1]()
      elif option =="2":
          options_toselect[2]()
      elif option =="3":
          options_toselect[3]()
      elif option =="4":
          options_toselect[4]()
      elif option =="5":
          options_toselect[5]()
      elif option =="6":
          options_toselect[6]()
      elif option =="7":
          options_toselect[7]()
      elif option =="8":
          sys.exit(0)
      else:
          print "Please select a valid choice !!!\n"


if __name__ == '__main__':
  main()

