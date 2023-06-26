package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}


/**
 * @author ${user.name}
 */
object App {

  private def dev_part1(spark: SparkSession): Unit = {
    val app_name = "_c0"
    val sentiment_polarity = "_c3"

    // LOAD THE CSV
    var google_play_user_review = spark.read.csv("./google-play-store-apps/googleplaystore_user_reviews.csv")

    // REPLACE THE EMPTY FILL OR NULL VALUES FOR 0
    google_play_user_review = google_play_user_review.na.replace(sentiment_polarity, Map("nan" -> "0", "null" -> "0", "" -> "0"))

    // CREATING A BEW DATAFRAME WITH COL _c0 (APP NAME) and _c3 (Sentiment Polarity)
    var df1 = google_play_user_review.select(col(app_name), col(sentiment_polarity).cast("double"))
    df1 = df1.groupBy(app_name).avg(sentiment_polarity)

    // CHANGE COLUMN NAMES
    df1 = df1.toDF("App", "Average_Sentiment_Polarity")

    df1.show()
  }

  def main(args: Array[String]): Unit = {

    // Initialize Spark
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName("Spark2-Challenger")
      .getOrCreate()

    /** ********************************* */
    /** ************ PART 1 ************* */
    /** ********************************* */

    dev_part1(spark)

    /** ************************************* */
    /** ************ END PART 1 ************* */
    /** ************************************* */
  }
}
