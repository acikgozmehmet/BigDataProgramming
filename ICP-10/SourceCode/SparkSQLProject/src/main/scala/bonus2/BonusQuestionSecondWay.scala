package bonus2

import org.apache.avro.generic.GenericData.StringType
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

object BonusQuestionSecondWay {

  def main(args: Array[String]): Unit = {
    //    Set the log level only to print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder().appName("Bonus Question App").master("local[*]").getOrCreate()

    val sc =sparkSession.sparkContext
    val rdd = sc.textFile("data/survey.csv").filter(line => !line.contains("TimeStamp"))
                                                    .map( line => line.split(",")).map(line => spark.sql.Row(line:_*))

    val schemaUntyped = new StructType()
      .add("Timestamp", "string")
      .add("Age", "string")
      .add("Gender", "string")
      .add("Country", "string")
      .add("state", "string")
      .add("self_employed", "string")
      .add("family_history", "string")
      .add("work_interfere", "string")
      .add("no_employees", "string")
      .add("remote_work", "string")
      .add("tech_company", "string")
      .add("benefits", "string")
      .add("care_options", "string")
      .add("wellness_program", "string")
      .add("seek_help", "string")
      .add("anonymity", "string")
      .add("leave", "string")
      .add("mental_health_consequence", "string")
      .add("phys_health_consequence", "string")
      .add("coworkers", "string")
      .add("supervisor", "string")
      .add("mental_health_interview", "string")
      .add("phys_health_interview", "string")
      .add("mental_vs_physical", "string")
      .add("obs_consequence", "string")
      .add("comments", "string")

    val df = sparkSession.sqlContext.createDataFrame(rdd, schemaUntyped)
    df.show(10)
  }

}
