package TrendingHashtags

import org.apache.spark.{SparkConf, SparkContext}

object CountTrendingHashtags {

  def main(args: Array[String]): Unit = {
    // Create a Scala Spark Configuration.
    val conf = new SparkConf().setMaster("local[*]").setAppName("Count Trending Hashtags")

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    // Turn off all the warnings but ERROR
    sc.setLogLevel("ERROR")

    // Load our input data.
    val lines = sc.textFile("sample2.json")

    // Split up into words and filter the hashtags
    val hashtags  = lines.flatMap(line => line.split(" ")).filter(w => w.matches("^#[A-Za-z0-9_.-]+$"))

    // lowercase the hashtags and Count the occurrences for each hashtag and sort then in descending order
    val trending  = hashtags.map(w => w.toLowerCase).map( w => (w,1)).reduceByKey( (x,y) => x + y).sortBy(_._2, false)

    // Save the trending hashtags back out to a text file, causing evaluation.
    trending.saveAsTextFile("output_trendingHashtags")
  }

}
