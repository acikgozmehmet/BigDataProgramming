package depthfirstsearch

import org.apache.spark.{SparkConf, SparkContext}

object DepthFirstSearch {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Breadthfirst").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    type Vertex = Int
    type Graph = Map[Vertex, List[Vertex]]
    val g: Graph = Map(1 -> List(2,3,5,6,7), 2 -> List(1,3,4,6,7), 3 -> List(1,2), 4 -> List(1,2,5,6),5 -> List(1,4),6 -> List(1,4,2),7 -> List(1,2))

    def DFS(start: Vertex, g: Graph): List[Vertex] = {
      def DFS0(vertex: Vertex, visited: List[Vertex]): List[Vertex] = {
        if (visited.contains(vertex)) {
          visited
        }
        else {
          val newNeighbor = g(vertex).filterNot(visited.contains)
          println(newNeighbor)
          newNeighbor.foldLeft(vertex :: visited)((b, a) => DFS0(a, b))
        }
      }

      DFS0(start, List()).reverse
    }

    val dfsresult1 = DFS(1, g)
    println("DFS Output starting at 1 : "+ dfsresult1.mkString(","))
    val dfsresult2 = DFS(2, g)
    println("DFS Output starting at 2 :" + dfsresult2.mkString(","))
    val dfsresult3 = DFS(3, g)
    println("DFS Output starting at 3 :"+ dfsresult3.mkString(","))
    val dfsresult4 = DFS(4, g)
    println("DFS Output starting at 4 :"+ dfsresult4.mkString(","))
    val dfsresult5 = DFS(5, g)
    println("DFS Output starting at 5 : "+ dfsresult5.mkString(","))
  }

}
