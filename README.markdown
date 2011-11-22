This is a quick&dirty implementation of a MongoDB storage handler for Apache HIVE.

#**CAUSION:**

* currently only support Hive primitive types: string, int, smallint....

* Whitespace should not be used in between entries in the "mongo.column.mapping" string, since these will be interperted as part of the column name, which is not what you want.

* field with name "id" in hive table will be mapped to "_id" field (Object Id) in MongoDB collections; so if you want "insert overwrite" feature, you must have a field named "id" and which is the "primary key".

Some code are borrowed/referenced from Balshor's Google Spreadsheet Handler(https://github.com/balshor/gdata-storagehandler) and HyperTable Hive extension(http://code.google.com/p/hypertable/wiki/HiveExtension), thanks for the help.

#Sample Usage:

    > $HIVE_HOME/bin/hive --auxpath /home/yc.huang/mongo-java-driver-2.6.3.jar,/home/yc.huang/guava-r06.jar,/home/yc.huang/hive-mongo-0.0.1-SNAPSHOT.jar

    

    hive> create external table mongo_users(id int, name string, age int) stored by "org.yong3.hive.mongo.MongoStorageHandler" with serdeproperties ( "mongo.column.mapping" = "id,name,age" ) tblproperties ( "mongo.host" = "192.168.0.5", "mongo.port" = "11211", "mongo.db" = "test", "mongo.collection" = "users" );

    OK
    Time taken: 4.093 seconds

    

    hive> insert overwrite table mongo_users select id, name,age from hive_test;

    

    Total MapReduce jobs = 1

    Launching Job 1 out of 1

    Number of reduce tasks is set to 0 since there's no reduce operator

    Starting Job = job_201111021553_13715, Tracking URL = http://JobTracker:50030/jobdetails.jsp?jobid=job_201111021553_13715

    Kill Command = /root/dev/hadoop-0.20.2/bin/../bin/hadoop job  -Dmapred.job.tracker=JobTracker:9001 -kill job_201111021553_13715

    2011-11-17 18:01:25,849 Stage-0 map = 0%,  reduce = 0%

    2011-11-17 18:01:28,876 Stage-0 map = 100%,  reduce = 0%

    2011-11-17 18:01:31,893 Stage-0 map = 100%,  reduce = 100%

    Ended Job = job_201111021553_13715

    4 Rows loaded to mongo_users

    OK

    Time taken: 14.37 seconds

    hive> select * from mongo_users;

    

    OK

    

    1       Tom     28

    2       Alice   18

    3       Bob     29

    101     Scott   10

    Time taken: 0.171 seconds

    