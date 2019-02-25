package hivetest

/**
  *
  */


import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

case class Record(key: Int, value: String)

object HiveOperation {

  def main(args: Array[String]) {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    //import spark.implicits._

    spark.sql("CREATE TABLE IF NOT EXISTS aaaaa (key INT, value STRING)")
    spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    val df = spark.sql("SELECT * FROM aaaaa")
    df.show()

    df.write.format("json").save("f://MyProject/spark_test/out_data/ssss.json")

    spark.stop()
  }


}



