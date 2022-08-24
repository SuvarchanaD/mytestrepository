package pack
import org.apache.spark.SparkContext

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row




object obj1 {
  println("==============rdd=====================")
  println
  println
  case class schema(txnno:String,
                    txndate:String,
                    custno:String,
                    amount:String,
                    category:String,
                    product:String,
                    city:String,
                    state:String,
                    spendby:String
                    )
  def main(args:Array[String]):Unit={
    
     val conf=new SparkConf().setAppName("myfirstApp").
             setMaster("local[*]")
  val sc=new SparkContext(conf)
  sc.setLogLevel("error")
  val spark=SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._
  val mylist= List(1,4,6,7)
  val itl=mylist.map(x=>x+2)
  itl.foreach(println)
  val listcol=List("txnno",
                    "txndate",
                    "custno",
                    "amount",
                    "category",
                    "product",
                    "city",
                    "state",
                    "spendby")
  
 println("==============rdd=====================")
  println
  println
  val mylist2=List("Zeyo","Zeyobron","suniZey")
  val fil=mylist2.filter(x=>x.contains("Zeyo"))
  fil.foreach(println)
  println("==============using file rdd=====================")
  println
  println
 
    
  val rdd1=sc.textFile("file:///C:/Bigdata_practice/revdata/revdata/file1.txt")
  val rdd2=rdd1.filter(x=>x.contains("gymnastics"))
  //rdd2.take(5).foreach(println)
   println("==============schema rdd=====================")
  println
  println
   
  val rdd3=sc.textFile("file:///C:/Bigdata_practice/revdata/revdata/file1.txt")
  val rdd4=rdd3.map(x=>x.split(","))
  val rdd5=rdd4.map(x=>schema(x(0),x(1),x(2),x(3)
                           ,x(4),x(5),x(6),x(7),x(8)))
  val rdd6=rdd5.filter(x=>x.product.contains("Gym"))
 val rdd9= rdd6.take(5).foreach(println)
  println("==============row rdd=====================")
  println
  println
   
  val rdd7=sc.textFile("file:///C:/Bigdata_practice/revdata/revdata/file1.txt")
  val rdd8=rdd7.map(x=> x.split(","))
  val rowrdd1=rdd8.map(x=> Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
  
  
  println("==============row rdd,schema rdd to Df=====================")
  println
  println
  
  val schemardd=rdd6.toDF()
       val schemadf=schemardd.select(listcol.map(col):_*).show(5)
  val structschema=StructType(Array(
                      StructField("txnno",StringType,true),
                      StructField("txndate",StringType,true),
                StructField("custno",StringType,true), 
                  StructField("amount",StringType,true),
                      StructField("category",StringType,true),
                StructField("product",StringType,true),
                  StructField("city",StringType,true),
                      StructField("state",StringType,true),
                StructField("spendby",StringType,true)
                ))
   val rowdf1=spark.createDataFrame(rowrdd1,structschema)
                .select(listcol.map(col):_*)
   val rowdf=rowdf1.show(5)
   
  println("==============csv read =====================")
  println
  println
   
  val df1=spark.read.format("csv").option("header","true")
               .load("file:///C:/Bigdata_practice/revdata/revdata/file3.txt")
               .select(listcol.map(col):_*)
               
               
     val csvdf=df1.show(5)
     
  println("==============json read=====================")
  println
  println
   
     
    val jsondf=spark.read
                .format("json")
                .load("file:///C:/Bigdata_practice/revdata/revdata/file4.json")
                .select(listcol.map(col):_*)
                val jsonf1=jsondf.show(5)
                
  println("==============parquet read=====================")
  println
  println
                
     val pardf= spark.read
                .format("parquet")
                .load("file:///C:/Bigdata_practice/revdata/revdata/file5.parquet")
                .select(listcol.map(col):_*)
                val pardf1=pardf.show(5)
                
  println("==============Xml read=====================")
  println
  println
     
    val xmldf= spark.read
                .format("xml")
                .option("rowtag","txndata")
                .load("file:///C:/Bigdata_practice/revdata/revdata/file6")
                .select(listcol.map(col):_*)
               val xmldf1=xmldf.show(5)
               
    println("==============union of all df's=====================")
    println
    println           
   
    val uniondf=schemardd
                 .union(rowdf1)
                 .union(df1) 
                 .union(jsondf)
                 .union(pardf)
                 .union(xmldf)
                
                 
                 uniondf.show(5)
                 
     println("==============Rename of year union of all df's=====================")
    println
    println    
    
    val dsldf= uniondf.withColumn("txndate",expr("split(txndate,'-')[2]"))
                       .withColumnRenamed("txnname", "year")
                       .withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
                       .filter(col("txnno")>50000)
           dsldf.show()
           
        println("==============groupby and aggregate=====================")
    println
    println        
  
     val sumdf=dsldf.groupBy("category").agg(sum("amount").alias("total_amount"))
     sumdf.show(5)
      
        println("==============Avro write=====================")
    println
    println   
    
    val comdf=sumdf.write.format("avro").partitionBy("category").mode("append").
         save("file:///C:/Bigdata_practice/avrocat")
         println("=========data written============")
         
      println("==============flatten address=====================")
    println
    println    
    val complexdf = spark.read.format("json").option("multiLine","true")
		.load("file:///C:/Bigdata_practice/address.json")


		complexdf.show()
		complexdf.printSchema()

    val flatdf1=complexdf.withColumn("phone_numbers",explode(col("phone_numbers")))
                .select(

				col("age"),
				col("billing_address.*"),
				col("date_of_birth"),
				col("email_address"),
				col("first_name"),
				col("height_cm"),
				col("is_alive"),
				col("last_name"),
				col("phone_numbers.*"),
				col("shipping_address.*")



				)

		flatdf1.show()
		flatdf1.printSchema()
    
         
  
}
}