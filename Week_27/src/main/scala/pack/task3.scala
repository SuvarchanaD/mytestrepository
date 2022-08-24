package pack
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row


object task3 {
  case class schema(id:String,name:String,comp_name:String)
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setAppName("first").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark= SparkSession.builder().getOrCreate()
    import spark.implicits._
   val datd=sc.textFile("file:///C:/Bigdata_practice/rd.csv",1);
   // datd.foreach(println)
    val mapdata=datd.map(x=>x.split(","))
    val schrdd=mapdata.map(x=>schema(x(0),x(1),x(2)))
        schrdd.toDF.show()

   
  
    
   
  }
  
}