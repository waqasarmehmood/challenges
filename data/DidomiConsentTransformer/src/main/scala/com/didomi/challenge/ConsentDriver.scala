package com.didomi.challenge

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

object ConsentDriver {
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Invalid arguments, Please specify configuration file")
      System.exit(1)
    }

    Configuration.setConfig(args(0))
    val s3Path = Configuration.getConfigValue("consentEventsS3Path")
    val hiveDb = Configuration.getConfigValue("hiveDataBase")
    val datehour = new java.text.SimpleDateFormat("YYYY-MM-DD-HH").format(new DateTime())

    val spark = SparkSession
      .builder()
      .appName("ConsentTransformer")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.caseSensitive","false")

    //TODO W_M: remove datehour hardcoding
    //hardcoding for testing

    val transformer = new ConsentTransformer(spark);
    transformer.run(s3Path, hiveDb, "2021-01-23-10");
  }
}
