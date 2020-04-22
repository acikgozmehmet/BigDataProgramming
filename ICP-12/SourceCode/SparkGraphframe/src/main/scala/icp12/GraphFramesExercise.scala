package icp12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._

object GraphFramesExercise {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("GraphFrameApplication").master("local[*]").getOrCreate()

    // 1.	Import the dataset as a csv file and create data frames directly on import than create graph out of the data frame created.
    val station_df = spark.read.format("csv").option("header", true).load("data/201508_station_data.csv")
    station_df.show(10)

    val trip_df = spark.read.format("csv").option("header", true).load("data/201508_trip_data.csv")
    trip_df.show(10)


    //  2.	Concatenate chunks into list & convert to Data Frame
    station_df.select(concat(col("lat"), lit(","), col("long")).as("lat_long")).show(10, false);

    //  3.	Remove duplicates
    val stationDF = station_df.dropDuplicates()
    val tripDF = trip_df.dropDuplicates()

    //    4.	Name Columns
    val renamed_tripDF = tripDF.withColumnRenamed("Trip ID", "tripId")
      .withColumnRenamed("Start Date", "StartDate")
      .withColumnRenamed("Start Station", "StartStation")
      .withColumnRenamed("Start Terminal", "src")
      .withColumnRenamed("End Date", "EndDate")
      .withColumnRenamed("End Station", "EndStation")
      .withColumnRenamed("End Terminal", "dst")
      .withColumnRenamed("Bike #", "BikeNum")
      .withColumnRenamed("Subscriber Type", "SubscriberType")
      .withColumnRenamed("Zip Code", "ZipCode")

    //    5.	Output Data Frame
    stationDF.show(10, false)
    renamed_tripDF.show(10, false)


    //    6.	Create vertices
    val vertices = stationDF.select(col("station_id").as("id"),
      col("name"),
      concat(col("lat"), lit(","), col("long")).as("lat_long"),
      col("dockcount"),
      col("landmark"),
      col("installation"))


    val edges = renamed_tripDF.select("src", "dst", "tripId", "StartDate", "StartStation", "EndDate", "EndStation", "BikeNum", "SubscriberType", "ZipCode")
    edges.show(10, false)


    val g = GraphFrame(vertices, edges)


    //    7.	Show some vertices
    g.vertices.select("*").orderBy("landmark").show()


    //    8.	Show some edges
    g.edges.groupBy("src", "StartStation", "dst", "EndStation").count().orderBy(desc("count")).show(10)


    //    9.	Vertex in-Degree
    val in_Degree = g.inDegrees
    in_Degree.orderBy(desc("inDegree")).show(8, false)


    //    10.	Vertex out-Degree
    val out_Degree = g.outDegrees
    out_Degree.show(10)
    vertices.join(out_Degree, Seq("id")).show(10)


    //    11.	Apply the motif findings.
    val motifs = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)").show(10, false)


    //    Bonus
    //    1.Vertex degree
          g.degrees.show(10)

    //    2. what are the most common destinations in the dataset from location to location.
          g.edges.groupBy("src", "dst").count().orderBy(desc("count")).show(10)

    //    3. what is the station with the highest ratio of in degrees but fewest out degrees. As in, what station acts as almost a pure trip sink. A station where trips end at but rarely start from.
          val df1 = in_Degree.orderBy(desc("inDegree"))
          val df2 = out_Degree.orderBy("outDegree")
          val df = df1.join(df2, Seq("id"))
                           .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
          df.orderBy(desc("degreeRatio")).limit(10).show(5, false)



    //    4.Save graphs generated to a file.
          g.vertices.write.mode("overwrite").parquet("output_vertices")
          g.edges.write.mode("overwrite").parquet("output_edges")



    spark.stop()
  }

}
