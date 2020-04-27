package spark_thedefinitiveguide.chapter5

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, desc, expr, lit}

object Transformations {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("Experiments").getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv").option("header", true).option("inferSchema", true).load("resources/data/flight-data/2010-summary.csv")

    //
    // Selection

    // select by column names
    df.select("DEST_COUNTRY_NAME", "count").show(2)

    // select by column
    df.select(col("count")).show(2)  // col() takes a colName

    // select modified column
    df.select(col("count")*3).show(2)

    // select by expression
    df.select(expr("count*3 as my_count")).show(2)  // expr() takes a complex Expression (and wraps a Column around it)
    df.selectExpr("count*3 as my_count").show(2)    // (convenience version of the above)

    // can use wildcard to select all columns
    df.selectExpr("*", "count*3 as my_count").show(2)

    // selecting Literals
    df.select(expr("*"), lit("1").alias("one"), expr("2 as two")).show(2)

    //
    // Aggregation
    df.select(avg(col("count")).alias("avg_cnt")).show(10)
    df.select(avg("count").alias("avg_cnt")).show(10)
    df.selectExpr("avg(count) as avg_cnt").show(10)

    //
    // Adding Columns
    df.withColumn("one", lit(1)).show(2)
    df.withColumn("messed_up_count", col("count") + 3).show(2)

    //
    // Renaming Columns
    df.withColumnRenamed("count", "new_count").show(3)

    //
    // Removing Columns
    df.drop("count").show(10)

    //
    // Casting Columns
    df.withColumn("count_double", col("count").cast("double")).show(10)

    //
    // Filtering Rows

    // sql style with where()
    df.where("count < 2").show()
    df.where(col("DEST_COUNTRY_NAME") === "United States").show()
    // Note: must use === or =!= here (because == would simply evaluate to a boolean)
    // (result of comparing a column object to a string object)

    // or using filter()
    df.filter(col("count") < 2).show()

    // filter is more powerful and also accepts arbitrary predicates:
    df.filter( row => row.getAs[Int]("count") <2).show()

    // multiple conditions
    df.where("count>10 OR count<2").show()
    df.where((col("count")>10).or(col("count")<2)).show()

    // multiple wheres can also be chained (without performance impact)
    df.where("count>2").where("count<10").show()

    //
    // Selecting unique records
    df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().show()

    //
    // Sampling / Splitting
    df.sample(false, 0.02).show() // Random Sample
    val Array(smallDf, largeDf) = df.randomSplit(Array(0.25, 0.75))  // Random Split  (and unpack the result into 2 vars)

    // Combining Data Frames
    val df2 = spark.read.format("csv").option("header", true).option("inferSchema", true).load("resources/data/flight-data/2011-summary.csv")
    df.union(df2)  // note the two dataframes must have the same schema (same columns in the SAME ORDER)

    //
    // Sorting
    df.sort("count").show(20)
    df.sort(expr("(count * -1)  + 5")).show(20) // can also sort by arbitrary expression

    df.sort(expr("count desc")).show(20) // specify ordering
    df.sort(desc("count")).show(20)

    // Sort within partitions
    df.sortWithinPartitions("count").show(20)


  }


}
