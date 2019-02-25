package partitioner

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  *
  */
class CustomerPartitioner(numParts: Int) extends Partitioner {

  //覆盖分区数
  override def numPartitions: Int = numParts

  //覆盖分区号获取函数
  override def getPartition(key: Any): Int = {
    val ckey: String = key.toString
    ckey.substring(ckey.length - 1).toInt % numParts
  }
}

object CustomerPartitioner1 extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("partitioner")
  val sc = new SparkContext(conf)

  val data = sc.parallelize(List("aa.2", "bb.2", "cc.3", "dd.3", "ee.5"))
  val path = "f://MyProject/spark_test/out_data/partitioner"
  data.map((_, 1)).partitionBy(new CustomerPartitioner(5)).keys.saveAsTextFile(path)

  sc.stop()
}


