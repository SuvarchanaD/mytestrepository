package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object task {
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setAppName("first").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark=SparkSession.builder().getOrCreate()
    import spark.implicits._
    //val filem= sc.textFile("file:///C:/Bigdata_practice/datat.txt")
    val pardf=spark.read
                .load("file:///C:/Bigdata_practice/data.parquet")
      pardf.show()
          
  
}
}