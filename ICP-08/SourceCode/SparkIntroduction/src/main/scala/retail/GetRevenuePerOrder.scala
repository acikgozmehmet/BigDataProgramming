package retail

import org.apache.spark.{SparkConf, SparkContext}

object GetRevenuePerOrder {
  def main(args: Array[String]): Unit = {
    // Create a Scala Spark Configuration.
    val conf = new SparkConf().setMaster("local[*]").setAppName("Get revenue per order")

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    // Turn off all the warnings but ERROR
    sc.setLogLevel("ERROR")

    // Load our input data.
    val orderItems = sc.textFile("order_items.txt")

    // Split up lines into key-value pairs
    // getting (id, sales) to list the revenue for each order in the line
    // estimate the revenue for each order in the whole data and sort them by order_id
    // list product_id and revenue
    val revenuePerOrder = orderItems.map(line => (line.split(",")(1).toInt, line.split(",")(4).toFloat ))
                                    .reduceByKey(_ +_).sortByKey()
                                    .map(line => line._1 + " " + line._2)

    // Save the output back out to a text file, causing evaluation.
    revenuePerOrder.saveAsTextFile("output_retail")
  }
}
