package app

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

case class AISRecord(mmsi: Int, timestamp: Timestamp, lat: Double, lon: Double)

case class Trajectory(mmsi: Int, startTime: Timestamp, endTime: Timestamp, recordCount: Int) {
  def durationMs = endTime.getTime-startTime.getTime
}

object Utils {
  def minutesToMillis(i: Int): Long =  i *60 *1000

  def getDF(spark: SparkSession) = {
    val df = spark.read
      .option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/ais/aisdk_20170808_100mmsi.csv.gz")

    import spark.implicits._ // required for next lines
    df
      .selectExpr("mmsi","to_timestamp(timestamp, 'dd/MM/yyyy HH:mm:ss') as timestamp",  "latitude as lat", "longitude as lon")
      .as[AISRecord]
  }
}
