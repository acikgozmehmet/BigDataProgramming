package icp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
object SparkSQLExercise {
  def main(args: Array[String]): Unit = {
    //    Set the log level only to print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //    Create SparkSession and SQLContext
    val sparkSession = SparkSession.builder().appName("Spark SQL basic example").master("local[*]").getOrCreate()
    val SQLContext = sparkSession.sqlContext

    //  1.	Import the dataset and create data frames directly on import.
    val df = SQLContext.read.option("header", true).csv("data/survey.csv")
    //   To show the first 20 records
    df.show(20)

    //  2.	Save data to file. It will overwrite the file if the file still exists with column headers
        df.write.mode("overwrite").option("header","true").csv("output")

    //  3.	Check for Duplicate records in the dataset - First Approach-1
    val distinctDF = df.distinct()
    println("Total # of records : "+ df.count()+ "\tDistinct count: "+distinctDF.count())

    //  3.	Check for Duplicate records in the dataset - Second Approach
    df.createOrReplaceTempView("table")
    println("Here is the duplicate lines")
    SQLContext.sql("SELECT  Timestamp, Age, Gender, Country, state, self_employed, family_history, treatment, work_interfere, no_employees, remote_work, tech_company, benefits, care_options, wellness_program, seek_help, anonymity, leave, mental_health_consequence,phys_health_consequence, coworkers, supervisor, mental_health_interview, phys_health_interview, mental_vs_physical, obs_consequence, comments "
      + ", count(*) FROM table"
      + " GROUP BY Timestamp, Age, Gender, Country, state, self_employed, family_history, treatment, work_interfere, no_employees, remote_work, tech_company, benefits, care_options, wellness_program, seek_help, anonymity, leave, mental_health_consequence,phys_health_consequence, coworkers, supervisor, mental_health_interview, phys_health_interview, mental_vs_physical, obs_consequence, comments"
      + " HAVING COUNT(*) > 1").show()

    //    4.	Apply Union operation on the dataset and order the output by Country Name alphabetically.
    val femaleDF = df.filter("Gender LIKE 'f%' OR Gender LIKE 'F%' ")
    val maleDF   = df.filter("Gender LIKE 'm%' OR Gender LIKE 'M%' ")
    val records = maleDF.union(femaleDF).orderBy("Country")
    records.show(100)

     //    5.	Use Groupby Query based on treatment.
     df.groupBy("treatment").count().show(10)


//    Part – 2:
//    1.	Apply the basic queries related to Joins and aggregate functions (at least 2)
    val df1 = femaleDF.select("Age" ,"Country","Gender","state","family_history")
    val df2 = maleDF.select("Age" ,"Country","Gender","state", "benefits")
    val jointdf = df1.join(df2, df1("Country") === df2("Country"), "inner")
    jointdf.show(false)


    val udf = df2.union(df1)
    val uniondf =udf.withColumn("Age", udf.col("Age").cast(DataTypes.IntegerType))
    uniondf.orderBy("Country").show(200)

    uniondf.groupBy("Country").count().show()
    uniondf.groupBy("Country").mean("Age").show()

//    2.	Write a query to fetch 13th Row in the dataset.
    println(df.take(13).last)


    sparkSession.stop()
  }

}
