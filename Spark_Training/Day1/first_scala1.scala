// Databricks notebook source
var a:Int = 1
var b = 1:Int

a=10 

// COMMAND ----------

val c=10

c=12

// COMMAND ----------

lazy val a=10

a

// COMMAND ----------

lazy val variable_lazy = {println("hello world");5}
variable_lazy

// COMMAND ----------

variable_lazy

// COMMAND ----------

lazy val sum = 10 + b
val b=9
println(sum)

// COMMAND ----------

lazy var a=10

// COMMAND ----------

val `my name is shikha` = "singh"

// COMMAND ----------

val `import` = 10
print(`import`)

// COMMAND ----------

def square(a:Int): Int={
  return a*a
  //return is optional
}
square(2)



// COMMAND ----------

//string
val a="\t\nhello world"
val b="\t\n\u03BB"

// COMMAND ----------

def add(a:Int, b:Int) = a+b
print(add(6,7))

// COMMAND ----------

val `void` =100
val `false`=true
val `return`=90
if(`false`) `void` else `return`

// COMMAND ----------

def square(a:Int):Int ={
  a*a
}

def sq2(y:Int, takeFunctions: Int=> Int):Int={
  //takeFunctions is replaced by function square
  //square will call here (argument y)-> square(2)
  takeFunctions(y)
}


sq2(2,square)

// COMMAND ----------

// collections:

val new1= List(1,2,3,4,5,6,7)
new1

// COMMAND ----------

new1.reverse

// COMMAND ----------

new1.head

// COMMAND ----------

new1.tail

// COMMAND ----------

//array
var new2= Array(1,2,3,4,5)



// COMMAND ----------

var new2(0)=100
new2

// COMMAND ----------

// DBTITLE 1,First RDD-SC(spark context-> object databricks which is used to work with clusters)
//first rdd
//parallelize method

val data =List(1,2,3,4,5)
//parallelize method used to create rdd is lazy in nature
val creationRDD = sc.parallelize(data)

// COMMAND ----------

//get the result of rdd-> action on your rdd
creationRDD.collect()

// COMMAND ----------

//get total partitions for your data
creationRDD.partitions.size

// COMMAND ----------

val rdd_partition = sc.parallelize(List(1,2,3,4),2)



// COMMAND ----------

rdd_partition.partitions.size

// COMMAND ----------

//count is an action and returns no. of elements
rdd_partition.count()

// COMMAND ----------

// map-> tranformation
//using map to create a new rdd from existing rdd (rdd_partition)
val maprdd=rdd_partition.map( x=> x*x*x )
// maprdd.collect()
// displaying first 3 data
maprdd.take(3)

// COMMAND ----------

maprdd.filter(x=>x%2==0).collect()

// COMMAND ----------

val mainrdd=sc.parallelize(List("hey","hello","shikha","singh"))
mainrdd.collect()

// COMMAND ----------

// map vs flatMap
mainrdd.map(x=> x.split(",")).collect()

// COMMAND ----------

mainrdd.flatMap(x=> x.split(",")).collect()

// COMMAND ----------

// creating rdd for key value pair
val rdd0=sc.parallelize(Array("one","two","three","one","two"))

// COMMAND ----------

val keydd= rdd0.map(x =>(x,1))

keydd.collect()

// COMMAND ----------

keydd.reduceByKey(_+_).collect()
