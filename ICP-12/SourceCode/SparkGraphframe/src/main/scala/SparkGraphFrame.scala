import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object SparkGraphFrame {
  def main(args: Array[String]) {
//    System.setProperty("hadoop.home.dir", "C:\\winutils")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()


    val input = spark.createDataFrame(List(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
      ("d", "David", 29),
      ("e", "Esther", 32),
      ("f", "Fanny", 36),
      ("g", "Gabby", 60)
    )).toDF("id", "name", "age")
    val output = spark.createDataFrame(List(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow"),
      ("f", "c", "follow"),
      ("e", "f", "follow"),
      ("e", "d", "friend"),
      ("d", "a", "friend"),
      ("a", "e", "friend")
    )).toDF("src", "dst", "relationship")

    val g = GraphFrame(input,output)
    g.vertices.show()
    g.edges.show()
  }
}
