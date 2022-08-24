package pack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object obj2 {
  def main(args:Array[String]):Unit={
      val conf=new SparkConf().setAppName("first APP")
                .setMaster("local[*]")
      val sc=new SparkContext(conf)
      val spark=SparkSession.builder().getOrCreate()
      import spark.implicits._
      println("============= Raw Data frame==================")
      val df=spark.read.format("json").option("multiline","true")
             .load("file:///C:/Bigdata_practice/jc5.json")
             df.show()
             println("============= RawSchema==================")
             df.printSchema()
     println("============= Transformed Data frame==================")
      val expdf=df.withColumn("students",explode(col("students")))
      expdf.show()
      expdf.printSchema()
      println("===============final data=====================")
      
      val finaldf=expdf.select(
          col("students.*"),
          col("doorno"),
          col("orgname"),
          col("trainer")
          )
         // finaldf.show()
          //finaldf.printSchema()
  }
}