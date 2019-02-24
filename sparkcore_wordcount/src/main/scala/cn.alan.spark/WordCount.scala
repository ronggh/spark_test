package cn.alan.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Spark Word Count
  */
object WordCount {

  val logger = LoggerFactory.getLogger(WordCount.getClass)

  def main(args: Array[String]) {
    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf)


    //使用sc创建RDD并执行相应的transformation和action
    // read from file
    val words = sc.textFile("file:///f://MyProject/spark_test/in_data/readme.txt")
    val counter = words.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_, 1).sortBy(_._2, false)
    // save to file
    counter.saveAsTextFile("file:///f://MyProject/spark_test/out_data/word_count")
    //停止sc，结束该任务
    logger.info("complete!")

    sc.stop()

  }

}








