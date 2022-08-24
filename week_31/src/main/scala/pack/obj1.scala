package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions


object obj1 {
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setAppName("first").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("error")
    val spark=SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df1=spark.read.format("csv").option("header","true").load("file:///C:/Bigdata_practice/product.csv")
    df1.show()
    val df2=spark.read.format("csv").option("header","true").load("file:///C:/Bigdata_practice/name.csv")
    df2.show()
    println("=================inner================")
    val joindf=df1.join(df2 , df1("id")===df2("name_id"),"inner").drop("name_id")
    joindf.show()
    println("=================left=================")
    val joindf1=df1.join(df2 , df1("id")===df2("name_id"),"left").drop("name_id")
    joindf1.show()
    println("=================left semi=================")
    val joindf6=df1.join(df2 , df1("id")===df2("name_id"),"left_semi").drop("name_id")
    joindf6.show()
     println("=================right=================")
    
    val joindf2=df1.join(df2 , df1("id")===df2("name_id"),"right").drop("id")
    joindf2.show()
     println("=================left_anti=================")
    
    val joindf3=df1.join(df2 , df1("id")===df2("name_id"),"left_anti").drop("name_id")
    joindf3.show()
    println("=================left_anti=================")
    val joindf4=df1.join(df2 , df1("id")===df2("name_id"),"outer")
    joindf4.show()
    
    
    
    
    
  }
  
}