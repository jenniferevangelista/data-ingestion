
## initialize pyspark 1.6 with this jars because this version not support operations with csv
sudo -u hdfs pyspark --jars /home/cloudera/Desktop/commons-csv-1.4.jar,/home/cloudera/Desktop/spark-csv_2.10-1.5.0.jar,/home/cloudera/Desktop/spark-sql_2.11-1.4.1.jar,/home/cloudera/Desktop/univocity-parsers-2.4.1.jar

#if usign the cloudera vm no required  import hivecontext
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc).config("hive.metastore.uris","thrift://localhost:9083")

#load arquive csv
dataFrame = sqlContext.read.format('com.databricks.spark.csv').options(delimiter=";", header="true").load("path_in_hdfs/arquive.csv")

#show dataframe  
dataFrame.show(5)

#print schema
dataFrame.printSchema()

#loading csv in hive table
sqlContext.registerDataFrameAsTable(dataframe, 'database.table')
dataFrame.write.insertInto("database.table", mode="overwrite")
