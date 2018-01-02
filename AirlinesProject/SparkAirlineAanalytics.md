# How Apache Spark can give wings to airline analytics?

The global airline industry continues to grow rapidly, but consistent and robust profitability is yet to be seen. According to the International Air Transport Association (IATA), the industry has doubled its revenue over the past decade, from US$369 billion in 2005 to an expected $727 billion in 2015.

In the commercial aviation sector, every player in the value chain — airports, airplane manufacturers, jet engine makers, travel agents, and service companies turns a tidy profit.

All these players individually generate extremely high volumes of data due to higher churn of flight transactions. Identifying and capturing the demand is the key here which provides much greater opportunity for airlines to differentiate themselves. Hence, Aviation industries can utilize big data insights to boost up their sales and improve profit margin.

#### Big data is a term for collection of datasets so large and complex that its computing cant be handled by traditional data processing systems or on-hand DBMS tools.

#### Apache Spark is an open source, distributed cluster computing framework specifically designed for interactive queries and iterative algorithms.

The Spark DataFrame abstraction is a tabular data object similar to R's native dataframe or Pythons pandas package, but stored in the cluster environment.

According to [Fortune's](http://fortune.com/2015/09/25/apache-spark-survey/ "Fortune")
 latest survey , Apache Spark is most popular technology of 2015.

Biggest Hadoop vendor Cloudera is also saying  GoodBye to Hadoops MapReduce and Hello to Spark .

What really gives Spark the edge over Hadoop is speed. Spark handles most of its operations in memory – copying them from the distributed physical storage into far faster logical RAM memory. This reduces the amount of time consumed in writing and reading to and from slow, clunky mechanical hard drives that needs to be done under Hadoops MapReduce system.

Also, Spark includes tools (real-time processing, machine learning and interactive SQL) that are well crafted for powering business objectives such as analyzing real-time data by combining historical data from connected devices, also known as the Internet of things.

Today, lets gather some insights on sample airport data using Apache Spark.

In previous blog we saw how to handle structured and semi-structured data in Spark using new Dataframes API and also covered how to process JSON data efficiently.

In this blog we will understand how to query data in DataFrames using SQL as well as save output to filesystem in CSV format.

### Using Databricks CSV parsing library

For this I am going to use a CSV parsing library provided by Databricks , a company founded by Creators of Apache Spark and which handles Spark Development and distributions currently.

Spark community consists of roughly 600 contributors who make it the most active project in the entire Apache Software Foundation, a major governing body for open source software, in terms of number of contributors.

Spark-csv library helps us to parse and query csv data in the spark. We can use this library for both for reading and writing csv data to and from any Hadoop compatible filesystem.

### Loading the data into Spark DataFrames

Lets load our input files into a Spark DataFrames using the spark-csv parsing library from Databricks.

You can use this library at the Spark shell by specifying --packages com.databricks: spark-csv_2.10:1.0.3

 
While starting the shell as shown below:

```$ bin/spark-shell --packages com.databricks:spark-csv_2.10:1.0.3```

Remember you should be connected to internet, because spark-csv package will get automatically downloaded when you give this command. I am using spark 1.4.0 version

Lets create sqlContext with already created SparkContext(sc) object

```
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._ 
```
Now lets load our csv data from airports.csv (airport csv github) file whose schema is as below


schema of airport  csv
```
scala> val airportDF = sqlContext.load("com.databricks.spark.csv", Map("path" -> "/home /poonam/airports.csv", "header" -> "true"))
```
The load operation will parse the *.csv file using Databricks spark-csv library and return a dataframe with column names same as in the first header line in file.

The following are the parameters passed to load method:

1. Source: "com.databricks.spark.csv"  tells spark we want to load as csv file.
2. Options:
  +  path – path of file, where it is located.
  +  Header: "header" -> "true" tells spark to map first line of file to column names for resulting dataframe.

Lets see what is schema of our Dataframe
table1

Check out sample data in our dataframe

```
scala> airportDF.show
```
### Querying CSV data using temporary tables:

To execute a query against a table, we call the sql() method on the SQLContext.

We have created airports DataFrame and loaded CSV data, to query this DF data we have to register it as temporary table called airports.

```
scala> airportDF.registerTempTable("airports")
```
Let's find out how many airports are there in South east part in our dataset
```
scala> sqlContext.sql("select AirportID, Name, Latitude, Longitude from airports where Latitude<0 and Longitude>0").collect.foreach(println)
[1,Goroka,-6.081689,145.391881]
[2,Madang,-5.207083,145.7887]
[3,Mount Hagen,-5.826789,144.295861]
[4,Nadzab,-6.569828,146.726242]
[5,Port Moresby Jacksons Intl,-9.443383,147.22005]
[6,Wewak Intl,-3.583828,143.669186] 
```
We can do aggregations in sql queries on Spark
We will find out how many unique cities have airports in each country
```
scala> sqlContext.sql("select Country, count(distinct(City)) from airports group by Country").collect.foreach(println)
[Iceland,10]
[Greenland,4]
[Canada,131]
[Papua New Guinea,6] 
```

What is average Altitude (in feet) of airports in each Country?
```
scala> sqlContext.sql("select Country , avg(Altitude) from airports group by Country").collect
res6: Array[org.apache.spark.sql.Row] =Array(
[Iceland,72.8], 
[Greenland,202.75],
[Canada,852.6666666666666], 
[Papua New Guinea,1849.0]) 
```

Now to find out in each timezones how many airports are operating?
```
scala> sqlContext.sql("select Tz , count(Tz) from airports group by Tz").collect.foreach(println)
[America/Dawson_Creek,1]
[America/Coral_Harbour,3]
[America/Halifax,9]
[America/Toronto,48]
[America/Vancouver,19]
[America/Godthab,3]
[Pacific/Port_Moresby,6]
[Atlantic/Reykjavik,10]
[America/Thule,1]
[America/St_Johns,4]
[America/Winnipeg,14]
[America/Edmonton,27]
[America/Regina,10] 
```

We can also calculate average latitude and longitude for these airports in each country
```
scala> sqlContext.sql("select Country, avg(Latitude), avg(Longitude) from airports group by Country").collect.foreach(println)
[Iceland,65.0477736,-19.5969224]
[Greenland,67.22490275,-54.124131999999996]
[Canada,53.94868565185185,-93.950036237037]
[Papua New Guinea,-6.118766666666666,145.51532] 
```

Lets count how many different DSTs are there
```
scala> sqlContext.sql("select count(distinct(DST)) from airports").collect.foreach(println)
[4]
```

###Saving data in CSV format
Till now we loaded and queried csv data. Now we will see how to save results in CSV format back to filesystem.
Suppose we want to send report to client about all airports in northwest part of all countries.
Lets calculate that first.
```
scala> val NorthWestAirportsDF=sqlContext.sql("select AirportID, Name, Latitude, Longitude from airports where Latitude>0 and Longitude<0")
NorthWestAirportsDF: org.apache.spark.sql.DataFrame = [AirportID: string, Name: string, Latitude: string, Longitude: string]
```
And save it to CSV file
```
scala> NorthWestAirportsDF.save("com.databricks.spark.csv", org.apache.spark.sql.SaveMode.ErrorIfExists, Map("path" -> "/home/poonam/NorthWestAirports.csv","header"->"true"))
```

The following are the parameters passed to save method.

1. Source: it same as load method com.databricks.spark.csv which tells spark to save data as csv.
2. SaveMode: This allows user to specify in advance what needs to be done if the given output path already exists. So that existing data wont get lost/overwritten by mistake. You can throw error, append or overwrite. Here, we have thrown an error ErrorIfExists as we dont want to overwrite any existing file.
3. Options: These options are same as what we passed to load method. Options:
  * path – path of file, where it should be stored.
  * Header: "header" -> "true" tells spark to map column names of dataframe to first line of resulting output file.

### Converting other data formats to CSV
We can also convert any other data format like JSON, parquet, text to CSV using this library.

In previous blog we had created json data. you can find it on github
```
scala> val employeeDF = sqlContext.read.json("/home/poonam/employee.json")
```
lets just save it as CSV.
```
scala> employeeDF.save("com.databricks.spark.csv", org.apache.spark.sql.SaveMode.ErrorIfExists, Map("path" -> "/home/poonam/employee.csv", "header"->"true"))
```
### Conclusion:
In this post we gathered some insights on airports data using SparkSQL interactive queries
and explored csv parsing library from Spark

Next blog we will explore very important component of Spark i.e Spark Streaming.

Spark Streaming allows users to gather realtime data into Spark and process it as it happens and gives away results instantly.
