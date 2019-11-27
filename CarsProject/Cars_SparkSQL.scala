#spark-shell

#define RDD with raw data
val rawData=sc.textFile("path to/cars.txt")

# map columns and datatypes
case class cars(make:String, model:String, mpg:Integer, cylinders :Integer, engine_disp:Integer, horsepower:Integer, weight:Integer ,accelerate:Double,	year:Integer, origin:String)

val carsData=rawData.map(x=>x.split("\t"))
.map(x=>cars(x(0).toString,x(1).toString,x(2).toInt,x(3).toInt,x(4).toInt,x(5).toInt,x(6).toInt,x(7).toDouble,x(8).toInt,x(9).toString))

# convert RDD to DataFrame
val carDF = carsData.toDF

# count cars origin wise
carDF.groupBy($"origin").count.show

# filter out american cars
carDF.filter($"origin" === "American").show

# count total american cars
carDF.filter($"origin"=== "American").count

# register a temView
carDF.createOrReplaceTempView("cars")

# select everything
spark.sql("select * from cars").show

# count cars origin wise using SQL
spark.sql("select origin, count(1) as count from cars group by origin").show

# filter out american cars
spark.sql("select * from cars where origin = \"American\"").show