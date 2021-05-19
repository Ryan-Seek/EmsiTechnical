#This program was developed and tested in Debain Stretch release 9.13

In order to run this program the user has to install the pandas library with the following command: 'sudo pip install pandas.'

Furthermore, the user must unpack the sample data file named 'data' and put it into the Resources folder.


#Notes about this program
-The Expired Date and Posted Date are stored in the database as TEXT since there is not a DATE type in sqlite3
-Due to the requirement that this code stream the input data I did not have the code automatically unzip and load the sample data. 
-While this program is designed to open an existing database and run normally, the intention behind this implementation was to allow the user to run it on multiple varying input files. If ran multiple times on the same file the database will contain duplicates since it does not destroy the database if it already exists.