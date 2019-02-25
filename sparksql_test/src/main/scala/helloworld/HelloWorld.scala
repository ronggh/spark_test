package helloworld


import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


/**
  *
  */
object HelloWorld {

  val logger = LoggerFactory.getLogger(HelloWorld.getClass)

  def main(args: Array[String]) {
    //创建SparkConf()并设置App名称
    //创建SQLContext，需要通过SparkContext
    val sqlContext = SparkSession
      .builder()
      .appName("HelloWorld")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    //通过隐式转换将RDD操作添加到DataFrame上
    import sqlContext.implicits._
    //通过spark.read操作读取JSON数据

    val df = sqlContext.read.json("f://MyProject/spark_test/in_data/people.json")
    //show操作类似于Action，将DataFrame直接打印到Console上
    // Displays the content of the DataFrame to stdout
    //    df.show()
    df.show()
    //DSL 风格的使用方式中 属性的获取方法
    //    df.filter($"age" > 21).show()
    df.filter($"age" > 20).show()
    //将DataFrame注册为表
    //    df.createOrReplaceTempView("persons")
    df.createOrReplaceTempView("persons")
    //    spark.sql("SELECT * FROM persons where age > 21").show()
    sqlContext.sql("select * from persons where age > 20").show()

    sqlContext.stop()

  }

}


