package traits

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  val spark: SparkSession = {
      SparkSession
        .builder
        .appName("Tests")
        .master("local")
        .getOrCreate()
  }
}