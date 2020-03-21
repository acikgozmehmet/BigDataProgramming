package SecondarySorting

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object SortTemperatureByDate {

  // function to parse the input file
  def parseLine(line: String) = {
    val fields = line.split(",")
    val year = fields(0).toInt
    val month = fields(1).toInt
    val day = fields(2).toInt
    val temperature = fields(3).toInt

    val k = (year + "-" + month)
    val k2 = (k, temperature)
    (k2, temperature)
  }

  def main(args: Array[String]): Unit = {
    // Create a Scala Spark Configuration.
    val conf = new SparkConf().setMaster("local[*]").setAppName("Sort Temperature By Date")

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    // Turn off all the warnings but ERROR
    sc.setLogLevel("ERROR")

    // Load our input data.
    val lines = sc.textFile("temperature.txt")

    // Split up lines into key and value pairs
    val rdd = lines.map(parseLine)

    // To partition the rdd into 2 clusters
    val partitionedRDD = rdd.partitionBy(new HashPartitioner(2))

    //  To map the rdd into key, value pairs and grouping values by date (key) and sorting the values after putting them into List
    val listRDD = partitionedRDD.map(l => (l._1._1, l._2)).groupByKey().mapValues(k => k.toList.sortBy(r => r))

    // printing on the console
    listRDD.collect().foreach(println)

    // Save the output back out to a text file, causing evaluation.
    listRDD.saveAsTextFile("output_secondarySorting")
  }


}
