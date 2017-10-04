

DATABASE ENGINE:

Developed in a Linux machine using eclipse and tested locally in command line and csgrads1 server of UTD.

Commands to run the program:

$ cd <project_folder>/bin
$ java DavisBaseMain

This will give a command prompt. I have also included a text file(samplerun.txt) of output of sample test run of different commands.

Commands supported:

$ SHOW DATABASE;
By default it has two database catalog and defaultdb. catalog is for metadata and in defaultdb you can run different commands.
if you havent created any new database it will be in defaultdb

$ CREATE DATABASE <database_name>;
ex: CREATE DATABASE varun;

$ DROP DATABASE <database_name>;
ex: DROP DATABASE varun;

$ USE DATABASE <database_name>;
ex: USE DATABASE varun;
Now this database will used for storing and manipulation. Any addition to any table can be seen only from
this database

$ SHOW TABLES;
This will output all the tables in davisbase with table name and corresponding database it is present in.

$ DROP TABLE <database_name>;
ex: DROP TABLE stud;
Here the table of the database which is in use will be deleted. Other database with same table name will be present.

create table:
$ CREATE TABLE table_name (
 column_name1 INT PRIMARY KEY,
 column_name2 data_type2 [NOT NULL],
 column_name3 data_type3 [NOT NULL],
 ...
);

ex: $ CREATE TABLE stud ( RollNo INT PRIMARY KEY, Name TEXT [NOT NULL], Grade REAL [NOT NULL], Major TEXT );
A new table will be created in the database which is in use.


Insert into table:
$ INSERT INTO TABLE (column_list) table_name VALUES (value1,value2,value3,…);
$ INSERT INTO table_name VALUES (value1,value2,value3,…);
either of these two formats can be used

$ SELECT *
  FROM table_name
  WHERE column_name operator value;
ex: SELECT * FROM stud WHERE GRADE > 3.8;
Here operators supported are '=','>','<','>=','<='. 
Displays all the collums and its corresponding values

$ DELETE FROM TABLE table_name WHERE column_name operator value;
ex : DELETE FROM TABLE stud WHERE Major = CS;
Here operators supported are '=','>','<','>=','<='.
Deletes a row that matches the condition

$ UPDATE table_name SET column_name operator value where [[column_name operator value] operator2 [column_name operator value]];
UPDATE stud SET grade = 3.5 where RollNo = 2;
Here operators supported are '=','>','<','>=','<='.
Additionally AND or OR can be used for operator2 

UPDATE should only be done with the values matching the older values. (variable length update is not supported)


SAMPLE RUN:

varun@varun:~/Desktop/DavisBaseDBEngine/bin$ java DavisBaseMain 
--------------------------------------------------------------------------------
Welcome to DavisBaseLite
DavisBaseLite Version v1.0b(example)
Â©2016 Varun Muthanna

Type "help;" to display supported commands.
--------------------------------------------------------------------------------
davisbase_tables
davisql> SHOW TABLES;
table_name	DBname
davisbase_tables	catalog

davisbase_columns	catalog

davisql> CREATE TABLE stud ( RollNo INT PRIMARY KEY, Name TEXT [NOT NULL], Grade REAL [NOT NULL], Major TEXT );
davisql> INSERT INTO TABLE (RollNo, Name,Grade, Major) stud VALUES (1,varun,3.89,CS);
davisql> SELECT * FROM stud;
rollno	name	grade	major	
1	varun	3.89	cs	
davisql> INSERT INTO TABLE (RollNo, Name,Grade, Major) stud VALUES (2,varun,3.89,CE);
davisql> SELECT * FROM stud;
rollno	name	grade	major	
1	varun	3.89	cs	
2	varun	3.89	ce	
davisql> DELETE FROM TABLE stud WHERE Major = CS;
delete command
davisql> SELECT * FROM stud;
rollno	name	grade	major	
2	varun	3.89	ce	
davisql> UPDATE stud SET grade = 3.5 where RollNo = 2;
davisql> SELECT * FROM stud;
rollno	name	grade	major	
2	varun	3.5	ce	
davisql> SHOW DATABASE;
DBname
----------
catalog
defaultdb
davisql> SHOW TABLES;
table_name	DBname
davisbase_tables	catalog

davisbase_columns	catalog

stud	defaultdb

davisql> CREATE DATABASE varun;
davisql> USE DATABASE varun;
davisql> CREATE TABLE stud ( RollNo INT PRIMARY KEY, Name TEXT [NOT NULL], Grade REAL [NOT NULL], Major TEXT );
davisql> INSERT INTO TABLE (RollNo, Name,Grade, Major) stud VALUES (1,varun,3.89,CS);
davisql> SELECT * FROM stud WHERE GRADE > 3.8;
rollno	name	grade	major	
1	varun	3.89	cs	
davisql> SELECT * FROM stud;
rollno	name	grade	major	
1	varun	3.89	cs	
davisql> SHOW DATABASE;
DBname
----------
catalog
defaultdb
varun
davisql> SHOW TABLES;
table_name	DBname
davisbase_tables	catalog

davisbase_columns	catalog

stud	defaultdb

stud	varun

davisql> DROP TABLE stud;
davisql> SHOW TABLES;
table_name	DBname
davisbase_tables	catalog

davisbase_columns	catalog

stud	defaultdb

davisql> USE DATABASE defaultdb;
davisql> SELECT * FROM stud;
rollno	name	grade	major	
2	varun	3.5	ce	
davisql> SHOW DATABASE;
DBname
----------
catalog
defaultdb
varun
davisql> DROP DATABASE varun;
davisql> SHOW DATABASE;
DBname
----------
catalog
defaultdb
davisql>

  




















