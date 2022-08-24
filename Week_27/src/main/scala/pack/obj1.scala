package pack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object obj1 {
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setAppName("first").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark=SparkSession.builder().getOrCreate()
    import spark.implicits._
    //val filem= sc.textFile("file:///C:/Bigdata_practice/datat.txt")
    
     val avrodf=spark.read.format("avro")
                .load("file:///C:/Bigdata_practice/part.avro")
                 avrodf.show()    
                 
      avrodf.createOrReplaceTempView("txns")

    val procdf=spark.sql("select * from txns where age>10 ")
    procdf.show()
    val rdd1=procdf.rdd.map(x=>x.mkString(","))
    
   // procdf.write.format("parquet").save("file:///C:/Bigdata_practice/parquetc")
    println("============done============")
  
}
}