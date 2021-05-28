package esgi.exo

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.IntegerType

object FootballApp {
  def isFranceDomicile(matchName: String): Boolean = matchName.substring(0,8) match {
    case "France -" => true
    case _ => false
  }

  val toIsFranceDomicileUdf: UserDefinedFunction = udf[Boolean, String](isFranceDomicile)

  def bool2int(b:Boolean) = if (b) 1 else 0

  val bool2int_udf = udf(bool2int _)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("TP1").config("spark.master", "local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val footDf = spark
      .read
      .option("header","true")
      .option("sep",",")
      .option("mode", "DROPMALFORMED")
      .csv(args(0))

    // As it is from data.gouv, date format can change, so we format it to date
    val filteredFootDf = footDf
      .withColumnRenamed("X4", "match")
      .withColumnRenamed("X6", "competition")
      .withColumn("penalty_france", when(col("penalty_france").equalTo("NA"), 0))
      .withColumn("penalty_adversaire", when(col("penalty_adversaire").equalTo("NA"), 0))
      .filter(to_date(footDf("date"), "yyyy-MM-dd").geq(lit("1980-03-01")))
      .select(
        "match",
        "competition",
        "adversaire",
        "score_france",
        "score_adversaire",
        "penalty_france",
        "penalty_adversaire",
        "date"
      ).cache()

    filteredFootDf.show()

    val updatedFootDf = filteredFootDf
      .withColumn("is_domicile", toIsFranceDomicileUdf(col("match")))
      .cache()

    updatedFootDf.show()

    val aggAdversaryStatsFootDf = updatedFootDf
        .groupBy(updatedFootDf("adversaire"))
        .agg(
          avg(updatedFootDf("score_france").cast(IntegerType)).alias("but_moyen_france"),
          avg(updatedFootDf("score_adversaire").cast(IntegerType)).alias("but_moyen_adversaire"),
          count(updatedFootDf("adversaire")).alias("nb_match_joue"),
          count(updatedFootDf("competition").like("%Coupe du monde%")).alias("nb_coupedumonde"),
          count(when(col("is_domicile"), true)).alias("sum_france_domicile"),
          max(updatedFootDf("penalty_france").cast(IntegerType)).alias("max_penalty_france"),
          max(updatedFootDf("penalty_adversaire").cast(IntegerType)).alias("max_penalty_adversaire"),
        )

    val withComputedStatsFootDf = aggAdversaryStatsFootDf
        .withColumn("diff_penalty", aggAdversaryStatsFootDf("max_penalty_france") - aggAdversaryStatsFootDf("max_penalty_adversaire"))
        .withColumn("percent_of_france_domicile",  (aggAdversaryStatsFootDf("sum_france_domicile") / aggAdversaryStatsFootDf("nb_match_joue")) * 100)
        .cache()

    withComputedStatsFootDf.show()

    withComputedStatsFootDf.write
      .mode(SaveMode.Overwrite)
      .parquet(args(1))

    val fullFootDf = spark
      .read
      .parquet(args(1))

    val joinedDf = filteredFootDf.join(
      fullFootDf,
      filteredFootDf("adversaire") === fullFootDf("adversaire"),
    )
      .drop(fullFootDf("adversaire"))
      .cache()

    joinedDf.show();

    joinedDf
      .withColumn("year", year(col("date")))
      .withColumn("month", month(col("date")))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month").parquet("result.parquet")

    spark.stop()
}
}