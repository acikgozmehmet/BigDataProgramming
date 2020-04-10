package bonus1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BonusQuestionFirstWay {

  case class Person(age: Long, gender: String, country: String, state: String)

  def parseLine(line: String): Person = {
    val fields = line.split(",")
    val person: Person = Person(fields(1).toLong, fields(2), fields(3), fields(4))
    return person
  }

  def main(args: Array[String]): Unit = {
    //    Set the log level only to print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //    Create SparkSession
    val sparkSession = SparkSession.builder()
      .appName("Spark SQL Bonus-Question")
      .master("local[*]").getOrCreate()

    //  Create SQLContext
    //    val SQLContext = sparkSession.sqlContext
    val lines = sparkSession.sparkContext.textFile("D:\\sharedfolder\\BigDataProgramming\\ICP-10\\SourceCode\\SparkSQLProject\\data\\survey.csv")

    //  To skip the header
    val lines2 = lines.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

    val people = lines2.map(parseLine)


    import sparkSession.implicits._
    val p = people.toDF()
    p.filter(p("age")>=13 and p("age")<=20).select("age" , "gender","country", "state").show(10)

/*
    val schemaPeople = people.toDS
    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView("table")
    val teenagers = sparkSession.sql("SELECT * FROM table WHERE age >= 13 AND age <=20")
    teenagers.collect().foreach(println)
*/


  }

}
