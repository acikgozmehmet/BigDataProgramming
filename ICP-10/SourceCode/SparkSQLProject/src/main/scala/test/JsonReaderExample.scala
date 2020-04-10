package test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object JsonReaderExample {

  def main(args: Array[String]): Unit = {

    //    Set the log level only to print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder().appName("Bonus Question App").master("local[*]").getOrCreate()

    val jsonData1 = sparkSession.sqlContext.read.option("mode","PERMISSIVE").json("D:/sil/TweetFile1.txt")
//    jsonData1.printSchema()

//    jsonData1.show(10)
    jsonData1.select("id").distinct().show()

  }
}
