// Databricks notebook source
///FileStore/tables/Property_data.csv
val property_rdd=sc.textFile("/FileStore/tables/Property_data.csv")

// COMMAND ----------

val removeHeader=property_rdd.filter(x=> !x.contains("Bedrooms"))
removeHeader.take(10)

// COMMAND ----------

val roomrdd=removeHeader.map(x=> (x.split(",")(3).toInt,(1,x.split(",")(2).toDouble)))
roomrdd.take(3)

// COMMAND ----------

val reducedrdd=roomrdd.reduceByKey((x,y)=> (x._1 + y._1, x._2 +y._2))
reducedrdd.collect()

// COMMAND ----------

val finalrdd=reducedrdd.mapValues(data=> data._2/data._1)
finalrdd.collect()

// COMMAND ----------

for((bedRoom,avg)<-finalrdd.collect()) println(bedRoom+" "+avg)

// COMMAND ----------

finalrdd.saveAsTextFile("Property_final.csv")
