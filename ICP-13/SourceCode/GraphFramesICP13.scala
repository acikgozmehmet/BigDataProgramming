package icp13

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

object GraphFramesICP13 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("GraphFrameApplication").master("local[*]").getOrCreate()

    //    1.	Import the dataset as a csv file and create data frames directly on import than create graph out of the data frame
    //    created. port the dataset as a csv file and create data frames directly on import than create graph out of the data frame created.

    val stationDF = spark.read.format("csv").option("header", true).load("data/201508_station_data.csv")
    stationDF.show(10, truncate = false)

    val tripDF = spark.read.format("csv").option("header", true).load("data/201508_trip_data.csv")
    tripDF.show(10, truncate = false)


    val vertices = stationDF.select(col("name").as("id"),
                                    concat(col("lat"), lit(","), col("long")).as("lat_long"),
                                    col("dockcount"),
                                    col("landmark"),
                                    col("installation")
                                   )

    println("vertices")
    vertices.show(10, truncate = false)

    val edges = tripDF.select(col("Start Station").as("src"),
                              col("End Station").as("dst"),
                              col("Trip ID").as("tripId"),
                              col("Duration").as("duration"),
                              col("Bike #").as("bikeNum"),
                              col("Subscriber Type").as("subscriber"),
                              col("Zip Code").as("zipCode")
                              )
    println("edges")
    edges.show(10, truncate = false)

//    Create the graphframe
    val g = GraphFrame(vertices, edges)
    g.cache()


    //    2.	Triangle Count
    val triangleCount = g.triangleCount.run()

    println("Triangles")
    triangleCount.select(col("id"), col("landmark"),col("count")).show(10, false)


//    3.	Find Shortest Paths w.r.t. Landmarks
    val shortPath = g.shortestPaths.landmarks(Seq("2nd at Folsom", "Townsend at 7th")).run()
    shortPath.show(false)


//    4.	Apply Page Rank algorithm on the dataset.
    // Display resulting pageranks and final edge weights
    val stationPageRank = g.pageRank.resetProbability(0.15).tol(0.01).run()
    stationPageRank.vertices.select("id", "pagerank").show(false)
    stationPageRank.edges.select("src", "dst", "weight").show(false)


//    5.	Save graphs generated to a file.
    g.vertices.write.mode("overwrite").parquet("icp13_output_vertices")
    g.edges.write.mode("overwrite").parquet("icp13_output_edges")



//    Bonus
//    1.	Apply Label Propagation Algorithm
    val lpa = g.labelPropagation.maxIter(5).run()
    lpa.select("id", "label").show()

//     2.	Apply BFS algorithm
    val pathBFS = g.bfs.fromExpr("id = '2nd at Folsom'").toExpr("dockcount < 15").run()
    pathBFS.show(false)

    spark.stop()

  }
}
