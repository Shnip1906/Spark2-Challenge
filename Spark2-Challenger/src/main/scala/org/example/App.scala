package org.example

import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, collect_list, concat_ws, count, explode, explode_outer, split, substring, to_date, trim, upper, when}
import org.apache.spark.sql.types.{DecimalType, DoubleType, LongType}


/**
 * @author ${user.name}
 */
object App {

  def main(args: Array[String]): Unit = {

    // Initialize Spark
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName("Spark2-Challenger")
      .getOrCreate()

    // LOAD THE CSV
    var google_play_store = spark.read.csv("./google-play-store-apps/googleplaystore.csv")
    val google_play_user_review = spark.read.csv("./google-play-store-apps/googleplaystore_user_reviews.csv")

    // DEFINE THE OUTPUT FILE NAME
    val outputFileDF2 = "best_apps"
    val outputFileDF3 = "googleplaystore_cleaned"
    val outputFileDF4 = "googleplaystore_metrics"


    /** ********************************* */
    /** ************ PART 1 ************* */
    /** ********************************* */

    // REPLACE THE EMPTY FIELD OR NULL VALUES FOR 0
    var df1 = google_play_user_review.na.replace("_c3", Map("nan" -> "0", "NaN" -> "0","null" -> "0", "" -> "0"))

    // CREATING A NEW DATAFRAME WITH COL _c0 (APP NAME) and _c3 (Sentiment Polarity)
    df1 = df1.select(col("_c0"), col("_c3").cast(DoubleType))
    df1 = df1.groupBy("_c0").avg("_c3")

    // CHANGE COLUMN NAMES
    df1 = df1.toDF("App", "Average_Sentiment_Polarity")

    df1.show()

    /** ********************************* */
    /** ************ PART 2 ************* */
    /** ********************************* */

    // REPLACE THE EMPTY FIELD OR NULL VALUES FOR 0
    var df2 = google_play_store.na.replace("_c2", Map("nan" -> "0", "NaN" -> "0", "null" -> "0", "" -> "0"))

    // CHANGE THE TYPE OF THE COLUMN _c2 (RATING)
    df2 = df2.withColumn("_c2", col("_c2").cast(DoubleType))

    // RATING GREATER OR EQUAL TO 4.0 AND SORTED IN DESCENDING ORDER
    df2 = df2.filter(df2("_c2") >= 4.0).sort(col("_c2").desc)

    // SAVE IN NEW CSV
    df2.write
      .format("csv")
      .options(Map("header" -> "true", "delimiter" -> "§"))
      .mode("overwrite")
      .csv(outputFileDF2)


    /** ********************************* */
    /** ************ PART 3 ************* */
    /** ********************************* */

    // CREATE A DATAFRAME WITH APP AND CATEGORIES ARRAY
    var df3A = google_play_store.groupBy("_c0").agg(concat_ws(",", collect_list(col("_c1"))).as("_c1"))
    // CHANGE DATA TYPE OF CATEGORIES
    df3A = df3A.withColumn("_c1", split(col("_c1"), ",").cast("array<string>"))

    // CREATE A DATAFRAME WITH MAX NUM OF REVIEWS FOR EACH APP
    var df3B = google_play_store.withColumn("_c3", col("_c3").cast(DoubleType))
    df3B = df3B.groupBy("_c0").max("_c3")
    df3B = df3B.toDF("_c0", "_c3")

    // CREATE A TEMPORARY VIEW TO PERFORM SQL QUERIES
    google_play_store.createOrReplaceTempView("google_play_store")
    df3B.createOrReplaceTempView("df3b")
    // VERIFY THE VALUES OF THE TWO DATAFRAMES [google_play_store] and [df3b]
    google_play_store = spark.sql("SELECT gp.* FROM google_play_store gp, df3b b WHERE gp._c3 == b._c3 AND gp._c0 == b._c0 ")

    // CREATE A DATAFRAME WITH APP AND GENRES ARRAY
    var df3C = google_play_store.groupBy("_c0").agg(concat_ws(";", collect_list(col("_c9"))).as("_c9"))
    // CHANGE DATA TYPE OF GENRES
    df3C = df3C.withColumn("_c9", split(col("_c9"), ";").cast("array<string>"))

    // CREATE A TEMPORARY VIEW TO PERFORM SQL QUERIES
    google_play_store.createOrReplaceTempView("google_play_store")
    df3A.createOrReplaceTempView("df3a")
    df3C.createOrReplaceTempView("df3c")

    // VERIFY THE VALUES OF THE TWO DATAFRAMES [google_play_store], [df3a] and [df3c]
    var df3 = spark.sql(
      "SELECT gp._c0, a._c1, gp._c2, gp._c3, gp._c4, gp._c5, gp._c6, gp._c7, gp._c8, c._c9, gp._c10, gp._c11, gp._c12 " +
        "FROM google_play_store gp, df3a a, df3c c WHERE a._c0 == gp._c0 AND c._c0 == gp._c0")

    // DEFAULT VALUES
    df3 = df3.na.replace(Seq("_c2", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12"), Map("NaN" -> null))

    // CHANGE DATA TYPE OF THE COLUMNS
    // RATING
    df3 = df3.withColumn("_c2", col("_c2").cast(DoubleType))
    // REVIEWS
    df3 = df3.withColumn("_c3", col("_c3").cast(LongType))
    // SIZE
    df3 = df3.withColumn("_c4",
      when(substring(upper(col("_c4")), -1, 1) === "K", trim(upper(col("_c4")), "K").cast(DoubleType) / 1024)
        .otherwise(trim(upper(col("_c4")), "M")).cast(DoubleType))
    // PRICE
    df3 = df3.withColumn("_c7", trim(col("_c7"), "$").cast(DoubleType) * 0.9)
    // LAST UPDATED
    df3 = df3.withColumn("_c10", to_date(col("_c10"), "MMMM d, yyyy"))

    // RENAME COLUMNS
    df3 = df3.toDF("App", "Categories", "Rating", "Reviews", "Size", "Installs", "Type", "Price",
      "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version")

    // SORT BY APP COLUMN
    df3.sort("App").show()


    /** ********************************* */
    /** ************ PART 4 ************* */
    /** ********************************* */

    df3.createOrReplaceTempView("df3")
    df1.createOrReplaceTempView("df1")

    val df1_3 = spark.sql("SELECT df3.*, df1.Average_Sentiment_Polarity FROM df3, df1 WHERE df1.App == df3.App")

    df1_3.write
      .format("parquet")
      .option("compression", "gzip")
      .mode("overwrite")
      .save(outputFileDF3)

    df1_3.show()


    /** ********************************* */
    /** ************ PART 5 ************* */
    /** ********************************* */

    // CREATE A TEMPORARY VIEW TO PERFORM SQL QUERIES
    df3.createOrReplaceTempView("df3")
    df1.createOrReplaceTempView("df1")

    // VERIFY THE VALUES OF THE TWO DATAFRAMES [df3] and [df1]
    var df4 = spark.sql("SELECT df3.*, df1.Average_Sentiment_Polarity FROM df3, df1 WHERE  df1.App == df3.App")

    // SELECT THE PRETENDED COLUMNS
    df4 = df4.select(explode(col("Genres")).alias("Genres"), col("App"), col("Rating"), col("Average_Sentiment_Polarity"))

    // CREATE DATAFRAME THAT GROUPS BY GENRES AND PERFORM THE AGGREGATION WITH COUNT AND AVG AND CHANGE COLUMN NAMES
    df4 = df4.groupBy("Genres").agg(
      count("App").as("Count"),
      avg("Rating").as("Average_Rating"),
      avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity"),
    )

    df4.write
      .format("parquet")
      .option("compression", "gzip")
      .mode("overwrite")
      .save(outputFileDF4)

    df4.show()

  }
}
