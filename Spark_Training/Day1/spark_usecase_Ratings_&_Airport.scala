// Databricks notebook source
// /FileStore/tables/ratings.csv

//creating rdd from external datasets
val data=sc.textFile("/FileStore/tables/ratings.csv")

// COMMAND ----------

data.collect()

// COMMAND ----------

// get the rating values only which is in 3rd column
val ratingsData = data.map(x=> x.split(",")(2))
ratingsData.collect()

// COMMAND ----------

ratingsData.count()

// COMMAND ----------

ratingsData.countByValue()

// COMMAND ----------

val airport=sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

val port=airport.filter(x=> x.split(",")(3).contains("United States")).take(2)

// COMMAND ----------

airport.collect()

// COMMAND ----------

val airportData = airport.filter(x=> x.split(",")(3)=="\"United States\"")
airportData.collect()

// COMMAND ----------

airportData.take(2)

//foreach(println)

// COMMAND ----------

state.count

// COMMAND ----------

// DBTITLE 1,First Approach
def splitInput(x:String)={
  val dataSplit=x.split(",")
  val airportId=dataSplit(1)
  val cityname=dataSplit(2)
  (airportId,cityname)
  //(dataSplit(1))
}

// COMMAND ----------

// map 3 and 4 index

airportData.map(splitInput).take(10)

// COMMAND ----------

// DBTITLE 1,Second Approach
airportData.map(x=>{
  val splitData =x.split(",")
  splitData(1)+"--"+splitData(2)+"\n"
}).collect()

// COMMAND ----------

// DBTITLE 1,Task1_day1
val task1=airport.map(x=>x.split(","))
val result=task1.filter(x=>x(6)>40.toString || x(3).contains("Ireland"))
result.count
result.take(5)


// COMMAND ----------

result.saveAsTextFile("countryName.csv")

// COMMAND ----------

// DBTITLE 1,Task2_day1
val task2=airport.map(x=>x.split(","))
val fil1=task2.filter(x=>x(7).toFloat%2==0)


// COMMAND ----------

val res1=fil1.map( x => x(11))
res1.countByValue
