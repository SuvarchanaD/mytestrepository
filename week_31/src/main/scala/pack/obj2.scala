package pack
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._


object obj2 {

  def main(args:Array[String]):Unit=
  {
     val conf=new SparkConf().setAppName("first").setMaster("local[*]")
                    .set("fs.s3a.access.key","AKIAUDT4DPHY7POJVVC2")
		            .set("fs.s3a.secret.key","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")
     val sc=new SparkContext(conf)
     sc.setLogLevel("error")
     val spark=SparkSession
                .builder()
                .config(conf)
                .getOrCreate()
     import spark.implicits._
     println("==============start=================")
     
    val df = spark.read.format("csv").option("header","true")
		.load("s3a://com.zeyo.dev/txns10k.txt")
		df.show()
		
    val sqldf = spark.read.format("jdbc")
.option("url", "jdbc:mysql://zeyodb1.curmotknfiq3.us-west-2.rds.amazonaws.com/zeyodb")
.option("driver", "com.mysql.jdbc.Driver")
.option("dbtable", "zeyotab")
.option("user", "root")
.option("password", "Aditya908")
.load()
sqldf.show()

     
 
     
  }
}