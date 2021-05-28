package esgi.exo

import org.apache.spark.sql._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import traits.SparkSessionWrapper

class FootballAppTest extends AnyFlatSpec with Matchers
  with SparkSessionWrapper {

  import spark.implicits._

  "filterFoot" should "Return a dataset with only after 01/03/1980 matchs" in {
    val data = List(
      ("17 novembre 1979","France - Tchécoslovaquie","2-1","Qualifications pour l'Euro 1980","Tchécoslovaquie","2","1","NA","NA","1979-11-17","1979","win","6"),
      ("27 février 1980,France - Grèce","5-1","Match amical","Grèce","5","1","NA","NA","1980-02-27","1980","win","1")
    ).toDF(
      "X2","X4","X5","X6","adversaire","score_france","score_adversaire","penalty_france","penalty_adversaire","date","year","outcome","no"
    )

    val result = FootballApp.filterFoot(data)

    val expectedOutput = List(
      ("27 février 1980,France - Grèce","5-1","Match amical","Grèce","5","1","NA","NA","1980-02-27","1980","win","1")
    )

    result.collect() should contain theSameElementsAs expectedOutput
  }
}
