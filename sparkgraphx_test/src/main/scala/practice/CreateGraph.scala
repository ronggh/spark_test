package practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateGraph {

  //屏蔽日志
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  def main(args: Array[String]) {

    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("CreateGraph")
    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf)

    // 创建一个顶点的集合，VertextId是类型参数，type VertexId = scala.Long
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // 创建一个边的集合
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val defaultUser = ("John Doe", "Missing")
    // 创建一张图,传入顶点集合和边集合，第三个参数是默认属性，如果没有设置，则采用该值
    val graph = Graph(users, relationships, defaultUser)

    // 过滤图的的有顶点，如果顶点属性第二个参数是postdoc，就过滤出来，并计数
    // Count all users which are postdocs
    val verticesCount = graph.vertices.filter {
      case (id, (name, pos)) => pos == "postdoc" }.count
    println("顶点属性为postdoc的数量：" + verticesCount)

    // 过滤图的所有边，如果源顶点大于目标顶点，则计数
    val edgeCount = graph.edges.filter(e => e.srcId > e.dstId).count
    println("源顶点大于目标顶点边个数为：" + edgeCount)

    println("顶点数：" + graph.numVertices)
    println("边数：" + graph.numEdges)
    println("入度：" + graph.inDegrees.count())
    println("出度：" + graph.outDegrees.count())

    sc.stop()

  }

}




