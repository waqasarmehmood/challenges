package com.didomi.challenge

import org.apache.spark.sql.functions.{avg, col, count, date_format, from_json, lit, size, struct, sum, to_timestamp, when}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

class ConsentTransformer(spark: SparkSession) {

  /**
   * Run the transformer to load hourly events and transform them to get aggregated information
   * @param basePath base network path
   * @param datehour YYYY-MM-dd-HH specific date hour to run the process
   * @param hiveDb Hive database to store aggregated information
   */
  def run(basePath:String, datehour:String, hiveDb:String): Unit ={

    println("RUNNING CONSENT DRIVER")
    println("S3Path: "+basePath)
    println("hiveDb: "+ hiveDb)
    println("datehour: "+ datehour);

    //load data
    val path = basePath+"/datehour="+datehour+"/"

    println("File Path:" + path)

    spark.sql(s"""
          CREATE TEMPORARY VIEW ConsentEvents
          USING org.apache.spark.sql.json OPTIONS
          ( path "${path}",
          multiline "true")""")

    val consent_df = spark.sql("SELECT * FROM ConsentEvents");

    //tranform
    val transformed_df = transform(consent_df);

    //save data
    transformed_df.write.format("delta").mode(SaveMode.Overwrite)
      .option("overwriteSchema", "true")
      .saveAsTable(hiveDb + ".Consents")
  }

  /**
   * Transform hourly consent events and extract consent metrics
   * @param consent_df dataframe containing raw consent events
   * @return transformed dataframe with metrics
   */
  def transform(consent_df: DataFrame) : DataFrame = {

    //Tranformations
    //1 - is positive consent
    //2 - datehour
    //3 - distinct user ids
    //4 - aggregate Metrics

    val tokenSchema = new StructType()
      .add("vendors", new StructType()
        .add("enabled", new ArrayType(StringType, true))
        .add("disabled", new ArrayType(StringType, true))
        , nullable = true)
      .add("purposes", new StructType()
        .add("enabled", new ArrayType(StringType, true))
        .add("disabled", new ArrayType(StringType, true))
        , nullable = true)

    //unique records
    val unique_events = consent_df.dropDuplicates("id")

    //Is positive consent
    val consentExtractedDF = unique_events.withColumn("parsedToken", from_json(col("user.token"), tokenSchema))
      .withColumn("user", struct(col("user.*"),  lit(size(col("parsedToken.purposes.enabled"))>0).as("consent"))).drop("parsedToken")

    //datehour
    val datehourDF = consentExtractedDF.withColumn("datehour", date_format(to_timestamp(col("datetime"), "YYYY-MM-dd HH:mm:ss"), "YYYY-MM-DD-HH"))

    //Aggregated Metrics
    val snapshot_df = datehourDF.groupBy("datehour","domain", "user.country")
      .agg(
        count(col("id")).alias("total_events"),
        sum(when(col("type") === "pageview", 1).otherwise(0)).alias("pageviews"),
        sum(when(col("type") === "pageview" && col("user.consent"), 1).otherwise(0)).alias("pageviews_with_consent"),
        sum(when(col("type") === "consent.asked", 1).otherwise(0)).alias("consents_asked"),
        sum(when(col("type") === "consent.asked" && col("user.consent"), 1).otherwise(0)).alias("consents_asked_with_consent"),
        sum(when(col("type") === "consent.given", 1).otherwise(0)).alias("consents_given"),
        sum(when(col("type") === "consent.given" && col("user.consent"), 1).otherwise(0)).alias("consents_given_with_consent"),
        avg(when(col("type") === "pageview", 1).otherwise(0)).alias("avg_pageviews_per_user"))
      .select("datehour","domain","country", "total_events", "pageviews","pageviews_with_consent", "consents_asked","consents_asked_with_consent", "consents_given","consents_given_with_consent", "avg_pageviews_per_user")

    snapshot_df
  }

}
