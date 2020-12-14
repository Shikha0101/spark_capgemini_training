// Databricks notebook source
val friendrdd=sc.textFile("/FileStore/tables/FriendsData.csv")

// COMMAND ----------

val removeHeader=friendrdd.filter(x=> !x.contains("Age"))
removeHeader.take(10)

// COMMAND ----------

// DBTITLE 1,Task1-Find Average
val agerdd=removeHeader.map(x=> (x.split(",")(2).toInt,(1,x.split(",")(3).toDouble)))
agerdd.take(3)

// COMMAND ----------

val reducedrdd=agerdd.reduceByKey((x,y)=> (x._1 + y._1, x._2 +y._2))
reducedrdd.collect()

// COMMAND ----------

val finalrdd=reducedrdd.mapValues(data=> math.round(data._2/data._1))
finalrdd.take(5)

// COMMAND ----------

for((age,avg)<-finalrdd.collect()) println(age+" "+avg)

// COMMAND ----------

// DBTITLE 1,Task2- Find Max
val agerdd1=removeHeader.map(x=> (x.split(",")(2).toInt,x.split(",")(3).toInt))
agerdd1.take(3)

// COMMAND ----------

//val reducerdd1=agerdd1.reduceByKey((x,y) => if(x._2 > y._2) x else y)
//val reducerdd1=agerdd1.reduceByKey((x,y) => x._2 > y._2 ? x._2 : y._2)
val reducerdd1=agerdd1.reduceByKey(math.max(_,_))
reducerdd1.collect()

// COMMAND ----------

for((age,max)<-reducerdd1.collect()) println(age+" "+max)
