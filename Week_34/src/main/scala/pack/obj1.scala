package pack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object obj1 {
  def main(args:Array[String]):Unit={
      val conf=new SparkConf().setAppName("first APP")
                .setMaster("local[*]")
      val sc=new SparkContext(conf)
      val spark=SparkSession.builder().getOrCreate()
      import spark.implicits._
      println("============= Raw Data frame==================")
      val df=spark.read.format("json").option("multiline","true")
             .load("file:///C:/Bigdata_practice/jc_34.json")
             df.show()
             println("============= RawSchema==================")
             df.printSchema()
      println("============= Transformed Data frame==================")
      val expdf=df.groupBy("doorno", "orgname","trainer")
                   .agg(collect_list("Students").alias("Students"))
          expdf.show();
          expdf.printSchema();
    
                 
  }
}