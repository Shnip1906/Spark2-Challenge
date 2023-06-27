package org.example

import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, split, substring, to_date, trim, upper, when}
import org.apache.spark.sql.types.{DecimalType, DoubleType, LongType}


/**
 * @author ${user.name}
 */
object App {

  private def dev_part1(sparkSession: SparkSession, csv: DataFrame): Unit = {
    val app_name = "_c0"
    val sentiment_polarity = "_c3"

    // REPLACE THE EMPTY FIELD OR NULL VALUES FOR 0
    var df1 = change_value_empty_fields(csv, sentiment_polarity, "0")

    // CREATING A NEW DATAFRAME WITH COL _c0 (APP NAME) and _c3 (Sentiment Polarity)
    df1 = df1.select(col(app_name), col(sentiment_polarity).cast("double"))
    df1 = df1.groupBy(app_name).avg(sentiment_polarity)

    // CHANGE COLUMN NAMES
    df1 = df1.toDF("App", "Average_Sentiment_Polarity")

    df1.show()
  }

  private def dev_part2(sparkSession: SparkSession, csv: DataFrame): Unit = {
    val rating = "_c2"

    // DEFINE THE OUTPUT FILE NAME
    val outputFileName = "best_apps"

    // REPLACE THE EMPTY FIELD OR NULL VALUES FOR 0
    var df2 = change_value_empty_fields(csv, rating, "0")

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

  private def dev_part3(sparkSession: SparkSession, csv: DataFrame): Unit = {
    val app = "_c0"
    val category = "_c1"
    val rating = "_c2"
    val reviews = "_c3"
    val size = "_c4"
    val installs = "_c5"
    val type_app = "_c6"
    val price = "_c7"
    val content_rating = "_c8"
    val genres = "_c9"
    val last_updated = "_c10"
    val current_version = "_c11"
    val android_version = "_c12"

    var google_play_store = sparkSession.read.csv("./google-play-store-apps/googleplaystore.csv")
    /*for (i <- google_play_store.columns.indices) {
      val columnName = google_play_store.columns(i)
      google_play_store = remove_empty_fields(google_play_store, columnName)
    }*/

    /*var df3 = csv.na.replace(rating, Map("NaN" -> null))
    // CHANGE DEFAULT VALUE OF REVIEWS
    df3 = df3.na.replace(reviews, Map("NaN" -> "0"))
    // CHANGE DEFAULT VALUE OF INSTALLS
    df3 = df3.na.replace(installs, Map("NaN" -> null))
    // CHANGE DEFAULT VALUE OF TYPE_APP
    df3 = df3.na.replace(type_app, Map("NaN" -> null))
    // CHANGE DEFAULT VALUE OF PRICE
    df3 = df3.na.replace(price, Map("NaN" -> null))
    // CHANGE DEFAULT VALUE CONTENT RATING
    df3 = df3.na.replace(content_rating, Map("NaN" -> null))
    // CHANGE DEFAULT VALUE GENRES
    df3 = df3.na.replace(genres, Map("NaN" -> null))
    // CHANGE DEFAULT VALUE LAST UPDATED
    df3 = df3.na.replace(last_updated, Map("NaN" -> null))
    // CHANGE DEFAULT VALUE CURRENT VERSION
    df3 = df3.na.replace(current_version, Map("NaN" -> null))
    // CHANGE DEFAULT VALUE ANDROID VERSION
    df3 = df3.na.replace(android_version, Map("NaN" -> null))

    // CHANGE DATA TYPE OF THR COLUMNS
      // CATEGORY
    df3 =  df3.withColumn(category, split(col(category), ";").cast("array<string>"))
      // RATING
    df3 = df3.withColumn(rating, col(rating).cast(DoubleType))
      // REVIEWS
    df3 = df3.withColumn(reviews, col(reviews).cast(LongType))
      // REMOVE LAST LETTER FROM SIZE -> Varies with device
    df3 = df3.withColumn(size,
      when(substring(upper(col(size)), -1, 1) === "K", trim(upper(col(size)), "K").cast(DoubleType) / 1024)
        .otherwise(trim(upper(col(size)), "M").cast(DoubleType)))
      // PRICE
    df3 = df3.withColumn(price, (trim(col(price), "$").cast(DoubleType) * 0.9).cast(DecimalType(18, 3)))
      // GENRES
    df3 =  df3.withColumn(genres, split(col(genres), ";").cast("array<string>"))
      // LAST UPDATED
    df3 = df3.withColumn(last_updated, to_date(col(last_updated), "MMMM d, yyyy"))
*/
  }

  private def change_value_empty_fields(csv: DataFrame, column: String, defaultValue: String): DataFrame = {
    val removed_fields = csv.na.replace(column, Map("nan" -> defaultValue, "NaN" -> defaultValue,"null" -> defaultValue, "" -> defaultValue))

    removed_fields
  }

  def main(args: Array[String]): Unit = {

    // Initialize Spark
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName("Spark2-Challenger")
      .getOrCreate()

    var google_play_store = spark.read.csv("./google-play-store-apps/googleplaystore.csv")
    val google_play_user_review = spark.read.csv("./google-play-store-apps/googleplaystore_user_reviews.csv")

    /** ********************************* */
    /** ************ PART 1 ************* */
    /** ********************************* */

    //dev_part1(spark, google_play_user_review)

    /** ********************************* */
    /** ************ PART 2 ************* */
    /** ********************************* */

    //dev_part2(spark, google_play_store)

    /** ********************************* */
    /** ************ PART 3 ************* */
    /** ********************************* */

    //dev_part3(spark, google_play_store)

    // CREATE A DATAFRAME WITH APP AND CATEGORIES ARRAY
    var df3A = google_play_store.groupBy("_c0").agg(concat_ws(",", collect_list(col("_c1"))).as("_c1"))
    // CHANGE DATA TYPE OF CATEGORIES
    df3A = df3A.withColumn("_c1", split(col("_c1"), ",").cast("array<string>"))

    // CREATE A DATAFRAME WITH APP AND GENRES ARRAY
    var df3C = google_play_store.groupBy("_c0").agg(concat_ws(";", collect_list(col("_c9"))).as("_c9"))
    // CHANGE DATA TYPE OF GENRES
    df3C = df3C.withColumn("_c9", split(col("_c9"), ";").cast("array<string>"))

    // CREATE A DATAFRAME WITH MAX NUM OF REVIEWS FOR EACH APP
    var df3B = google_play_store.withColumn("_c3", col("_c3").cast(DoubleType))
    df3B = df3B.groupBy("_c0").max("_c3")
    df3B = df3B.toDF("_c0", "_c3")

    // CREATE A TEMPORARY VIEW TO PERFORM SQL QUERIES
    google_play_store.createOrReplaceTempView("google_play_store")
    df3B.createOrReplaceTempView("df3b")
    // VERIFY THE VALUES OF THE TWO DATAFRAMES [google_play_store] and [df3b]
    google_play_store = spark.sql("SELECT gp.* FROM google_play_store gp, df3b b WHERE gp._c3 == b._c3 AND gp._c0 == b._c0 ")

    // VERIFY THE VALUES OF THE TWO DATAFRAMES [google_play_store], [df3a] and [df3c]
    google_play_store.createOrReplaceTempView("google_play_store")
    df3A.createOrReplaceTempView("df3a")
    df3C.createOrReplaceTempView("df3c")

    var df3 = spark.sql(
      "SELECT gp._c0, a._c1, gp._c2, gp._c3, gp._c4, gp._c5, gp._c6, gp._c7, gp._c8, c._c9, gp._c10, gp._c11, gp._c12 " +
        "FROM google_play_store gp, df3a a, df3c c WHERE a._c0 == gp._c0 AND c._c0 == gp._c0")

    // DEFAULT VALUES
    df3 = df3.na.replace("_c2", Map("NaN" -> null))
    df3 = df3.na.replace("_c3", Map("NaN" -> "0"))
    df3 = df3.na.replace("_c4", Map("NaN" -> null))
    df3 = df3.na.replace("_c5", Map("NaN" -> null))
    df3 = df3.na.replace("_c6", Map("NaN" -> null))
    df3 = df3.na.replace("_c7", Map("NaN" -> null))
    df3 = df3.na.replace("_c8", Map("NaN" -> null))
    df3 = df3.na.replace("_c9", Map("NaN" -> null))
    df3 = df3.na.replace("_c10", Map("NaN" -> null))
    df3 = df3.na.replace("_c11", Map("NaN" -> null))
    df3 = df3.na.replace("_c12", Map("NaN" -> null))

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
  }
}
