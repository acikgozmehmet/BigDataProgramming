package characacter_frequency

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object TCPStreamCharacterFrequency {
  def main(args: Array[String]): Unit = {

    // Create the context with a 5 second batch size
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkCharacterFrequencyCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    Logger.getLogger("org").setLevel(Level.ERROR)
    val data = ssc.socketTextStream("localhost",9999)
    val wc = data.flatMap(_.split(" ")).map(x => (x.length(), x)).groupByKey().filter( x => x._1 > 0).map( x => (x._1, x._2.toList.mkString(" [", ", ", "]")))
    wc.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
