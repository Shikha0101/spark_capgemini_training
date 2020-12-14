// Databricks notebook source
val airportrdd=sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

val datardd=airportrdd.map(x=>(x.split(",")(1),x.split(",")(3)))
datardd.take(2)

// COMMAND ----------

val newrdd=airportrdd.map(x=>(x.split(",")(1),x.split(",")(11).toLowerCase))
newrdd.take(5  )


// COMMAND ----------



// COMMAND ----------

val listData=List("Shikha 2020","Richa 1877","Singh 9866")

// COMMAND ----------

val res=sc.parallelize(listData)
val res1=res.map(x=>(x.split(" ")(0),x.split(" ")(1).toInt))
res1.take(3)

// COMMAND ----------

res1.mapValues(x=>x+10).take(3)
