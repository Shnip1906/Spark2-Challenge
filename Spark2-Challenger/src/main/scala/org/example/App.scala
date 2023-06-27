package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, when}
import org.apache.spark.sql.types.DoubleType


/**
 * @author ${user.name}
 */
object App {

  private def dev_part1(sparkSession: SparkSession): Unit = {
    val app_name = "_c0"
    val sentiment_polarity = "_c3"

    // LOAD THE CSV
    var google_play_user_review = sparkSession.read.csv("./google-play-store-apps/googleplaystore_user_reviews.csv")

    // REPLACE THE EMPTY FILL OR NULL VALUES FOR 0
    var df1 = google_play_user_review.na.replace(sentiment_polarity, Map("nan" -> "0", "NaN" -> "0", "null" -> "0", "" -> "0"))

    // CREATING A NEW DATAFRAME WITH COL _c0 (APP NAME) and _c3 (Sentiment Polarity)
    df1 = df1.select(col(app_name), col(sentiment_polarity).cast("double"))
    df1 = df1.groupBy(app_name).avg(sentiment_polarity)

    // CHANGE COLUMN NAMES
    df1 = df1.toDF("App", "Average_Sentiment_Polarity")

    df1.show()
  }

  private def dev_part2(sparkSession: SparkSession): Unit = {
    val rating = "_c2"

    // DEFINE THE OUTPUT FILE NAME
    val outputFileName = "best_apps"

    // LOAD THE CSV
    var google_play_store = sparkSession.read.csv("./google-play-store-apps/googleplaystore.csv")

    // REPLACE THE EMPTY FILL OR NULL VALUES FOR 0
    var df2 = google_play_store.na.replace(rating, Map("nan" -> "0", "NaN" -> "0","null" -> "0", "" -> "0"))

    // CHANGE THE TYPE OF THE COLUMN _c2 (RATING)
    df2 = df2.withColumn(rating, col(rating).cast(DoubleType)).as(rating)

    // RATING GREATER OR EQUAL TO 4.0 AND SORTED IN DESCENDING ORDER
    df2 = df2.filter(df2(rating) >= 4.0).sort(col(rating).desc)

    // SAVE IN NEW CSV
    df2.write
      .format("csv")
      .options(Map("header" -> "true", "delimiter"-> "§"))
      .mode("overwrite")
      .csv(outputFileName)
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

    /** ********************************* */
    /** ************ PART 2 ************* */
    /** ********************************* */

    dev_part2(spark)
  }
}
