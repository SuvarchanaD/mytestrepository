package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source

object obj1 {
  
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setAppName("my first app").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("error")
    val spark=SparkSession.builder().getOrCreate()
    import spark.implicits._
    val html=Source.fromURL("https://randomuser.me/api/0.8/?results=5")
    val urldata=html.mkString
    println(urldata)
    val rdd=sc.parallelize(List(urldata))
    val df=spark.read.json(rdd)
    df.show()
    df.printSchema()
    
    val flattendf=df.withColumn("results",explode(col("results")))
     val flatten2=flattendf.select(
                         col("nationality"),
                         col("results.user.cell"),
                         col("results.user.dob"),
                         col("results.user.email"),
                         col("results.user.gender"),
                         col("results.user.location.*"),
                         col("results.user.md5"),
                         col("results.user.name.*"),
                         col("results.user.password"),
                         col("results.user.phone"),
                         col("results.user.picture.*"),
                         col("results.user.registered"),
                         col("results.user.salt"),
                            col("results.user.sha1"),
                               col("results.user.sha256"),
                                  col("results.user.username"),
                                  col("seed"),
                                  col("version")
                                  )
                                
            flatten2.show()
           flatten2.printSchema()
      flatten2.write.format("csv").option("header","true")
      .partitionBy("nationality")
      .mode("append")
      .save("file:///C:/Bigdata_practice/week35_output/file1")
        
    
  }
  
}