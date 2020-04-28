package spark_thedefinitiveguide.chapter6

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, current_timestamp, date_add, date_sub, expr, lit, udf, unix_timestamp, from_unixtime, datediff, to_date, to_timestamp}

object DataTypes_Time {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("Experiments").getOrCreate()
    import spark.implicits._

    //
    // Basics
    //

    // Now
    val dateDf = spark.range(10).withColumn("today", current_date()).withColumn("now", current_timestamp())
    dateDf.show(false)

    // add a column with an explicit date
    val t = Timestamp.valueOf("2020-05-18 12:30:00")
    dateDf.withColumn("time", lit(t)).show()

    // convert a string column to date
    dateDf.withColumn("date", lit("2020-05-01")).select(to_date('date)).show(false)

    // convert string column to timestamp
    dateDf.withColumn("time", lit("2020-05-01 12:30:00")).select(to_timestamp('time)).show(false)

    // convert string column to timestamp with custom date format
    dateDf.withColumn("time", lit("01.05.2020 12:30:00")).select(to_timestamp('time, "dd.MM.yyyy HH:mm:ss")).show(false)


    //
    // Add / Subtract
    //

    // Adding/Subtracting days is easy
    dateDf.select(date_add('today, 5)).show()

    // Adding / Subtracting something other than days seems to be less well supported
    // Here are some versions

    // Using an expr
    dateDf.select('now + expr("INTERVAL 5 MINUTES")).show(false)

    // Using a UDF
    val add_minutes = udf {(time:Timestamp, minutes:Int) => new Timestamp(time.getTime + minutes*60*1000)}
    dateDf.select(add_minutes('now, lit(5))).show(false)  // need to convert minutes to a spark datatype using lit()

    // UDF version 2  (using currying)
    // that way we don't need lit() to convert minutes to spark, but maybe less readable?
    val addminutes = (minutes:Int) => udf {(time: Timestamp) => new Timestamp(time.getTime + minutes*60*1000)}
    dateDf.select(addminutes(5)('now)).show(false)

    // Using conversion to unix timestamp and back
    dateDf.select(from_unixtime(unix_timestamp('now) + 5*60)).show(false)  // Note: unix time is in seconds!

    //
    // Duration
    //

    // duration in days
    val dateDf2 = dateDf.withColumn("week_ago", date_sub('today, 7))
    dateDf2.select(datediff('week_ago, 'today)).show()

    // duration in seconds
    val timediff = udf {(t1:Timestamp, t2:Timestamp) => t1.getTime-t2.getTime}
    dateDf2.select(timediff('today, 'week_ago)).show()

    // duration in seconds v2
    dateDf2.select(unix_timestamp('today) - unix_timestamp('week_ago)).show()

    //
    // Comparisons / Filters
    //
    dateDf2.select('today > 'week_ago).show()
    dateDf2.select('today >= 'week_ago).show()
    dateDf2.select('today =!= 'week_ago).show()
    dateDf2.select('today === 'week_ago).show()

    // can directly compare against literal dates
    dateDf2.select('now > "2020-04-28 18:30:00").show()

    // Filter
    dateDf2.filter('now < "2020-04-28 18:30:00").show()
  }

}
