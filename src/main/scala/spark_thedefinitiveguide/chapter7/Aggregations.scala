package spark_thedefinitiveguide.chapter7

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions.{col, expr, lit, udf}
import org.apache.spark.sql.functions.{approx_count_distinct, avg, count, countDistinct, first, kurtosis, last, max, min, skewness, sum, var_samp}
import org.apache.spark.sql.functions.{collect_list, collect_set, to_date, rank, dense_rank, row_number, grouping_id}

object Aggregations {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("Experiments").getOrCreate()
    import spark.implicits._

    var df = spark.read.format("csv").option("header", true).option("inferSchema", true).load("resources/data/retail-data-by-day/*.csv")
    df.cache()
    df.createOrReplaceTempView("dfTable")

    // Counting
    df.count()
    df.select(count('StockCode)).show()
    df.select(countDistinct('StockCode)).show()

    df.select(approx_count_distinct('StockCode, 0.1)).show()  // 0.1 = maximum estimation error allowed

    // First/Last Min/Max
    df.select(first('StockCode), last('StockCode), min('StockCode), max('StockCode)).show()

    //
    // Simple Aggregation Functions
    //

    df.select(sum('Quantity)).show()  // or sumDistinct
    df.select(avg('Quantity)).show()
    df.select(var_samp('Quantity)).show()  // or var_pop for population Variance

    df.select(skewness('Quantity), kurtosis('Quantity)).show()
    // covariance, correlation ...

    //
    // Aggregating to complex types
    //
    df.select(collect_list('Quantity)).show(false)
    df.select(collect_set('Quantity)).show(false)


    //
    // Simple Grouping
    //
    val groupedDataset: RelationalGroupedDataset = df.groupBy('InvoiceNo)  // can use multiple columns here

    // now apply any aggregation to the groupedDataset

    // Aggregations on the whole Dataset
    groupedDataset.count().show()  // min / max / mean / ..

    // Aggregation using arbitrary expressions
    groupedDataset.agg(countDistinct('CustomerId), min('Quantity), max('Quantity), collect_set('StockCode)).show(false)

    // Aggregations can also be defined as a Map
    groupedDataset.agg("Quantity"->"count", "Quantity"->"min", "StockCode"->"collect_set").show()


    //
    // Grouping with Window Functions
    //

    val df2 = df.withColumn("date", to_date('InvoiceDate))
    df2.show()

    // Define a Window, then use it in a select() to control the input to aggregate functions
    val window = Window.partitionBy('CustomerId, 'date).orderBy('Quantity.desc).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df2.select('CustomerId, 'date, sum('Quantity).over(window), max('Quantity).over(window)).show()

    // contrary to groupBy we can also select columns which are not in the Grouping defined by Window.partitionBy()
    df2.select('CustomerId, 'date, 'Quantity, sum('Quantity).over(window), max('Quantity).over(window)).show()

    // (it's possible to use multiple different Window definitions in the same select)
    val window2 = Window.partitionBy('date).orderBy('Quantity.desc).rangeBetween(-3, 0)
    df2.select('CustomerId, 'date, sum('Quantity).over(window), sum('Quantity).over(window2)).show(false)


    //
    // Ranking Functions

    // Ranking functions are defined within a window (given by Window.partitionBy() ) with an order (defined by Window.orderBy())
    val window3 = Window.partitionBy('CustomerId).orderBy('Quantity.desc)

    // row_number : index of the row within the window
    df2.select('CustomerId, 'Quantity, row_number().over(window3)).show(100)

    // rank / dense_rank
    // rank also indicates the ordering position of the record within the window. But it handles TIES differently than row_number:
    //  - rank: when there are ties the next ranks are skipped:  1 2 3 3 5 6
    //  - dense_rank: leaves NO GAPS when there are ties: 1 2 3 3 4 5
    df2.select('CustomerId, 'Quantity, dense_rank().over(window3)).show(100)


    //
    // Grouping Sets  (only in SQL)
    //

    // first, need to eliminate null values from the grouping columns
    df.na.drop("any", Seq("CustomerId", "StockCode")).createOrReplaceTempView("dfTable2")

    spark.sql("SELECT CustomerId, stockCode, sum(Quantity) FROM dfTable2" +
      " GROUP BY customerId, StockCode" +
      " GROUPING SETS((customerId, stockCode), (stockCode))" +
      " ORDER BY CustomerId DESC, stockCode DESC").show(100000)

    // this will produce the "normal" grouped output you would expect, but also add an aggregate row for every stockCode (where customerId=null)
    // for details see https://www.waitingforcode.com/apache-spark-sql/grouping-sets-apache-spark-sql/read


    //
    // Rollups
    //

    // a Rollup "computes hierarchical subtotals from left to right” : https://mungingdata.com/apache-spark/aggregations/

    // first, need to eliminate nulls from the rollup columns
    val dfNotNull = df2.na.drop("any")

    val rollup:RelationalGroupedDataset = dfNotNull.rollup('date, 'Country)
    // this basically defines a nested loop:
    // for each date
    //      for each country
    //          create_row:  (date, country, aggregate( values(date, country) )
    //      create_row: (date, country=null, aggregate(values(date))
    // create_row: (date=null, country=null, aggregate(all_values))

    // now define the aggregates we want to execute over the rollup:
    val rollup_result = rollup.agg(sum('Quantity).as("sum")).select('date, 'Country, 'sum)

    rollup_result.where("date IS NOT NULL AND Country IS NOT NULL").show()  // one row for each country/date combination
    rollup_result.where("date IS NOT NULL AND Country IS NULL").show()  // one row per date
    rollup_result.where("date IS NULL AND Country IS NULL").show()  // grand total

    //
    // Cubes
    //

    // a Cube is similar to a Rollup but it
    // “applies the aggregate expressions to ALL POSSIBLE COMBINATIONS of the grouping columns”

    val cube:RelationalGroupedDataset = dfNotNull.cube('date, 'Country)
    val cube_result = cube.agg(sum('Quantity).as("sum")).select('date, 'Country, 'sum)

    cube_result.where("date IS NOT NULL AND Country IS NOT NULL").show()  // one row for each country/date combination
    cube_result.where("date IS NULL AND Country IS NOT NULL").show()  // one row for each country
    cube_result.where("date IS NOT NULL AND Country IS NULL").show()  // one row for each date
    cube_result.where("date IS NULL AND Country IS NULL").show()  // grand total


    //
    // Grouping Metadata
    //

    // grouping_id() adds a column that indicates the level of aggregation for each row (higher number = higher aggregation level)

    rollup.agg(grouping_id(), sum('Quantity)).show()
    // level 0 -> date!=null && country!=null  --> lowest level of aggregation (a row for each date/country combination)
    // level 1 -> date!=null && country==null
    // level 3 -> date==null && country=null  --> highest level of aggregation (only one row with aggregates for the entire data frame)


    //
    // Pivot
    //

    // ..allows to transform a row into a column

    val pivot:RelationalGroupedDataset = df2.groupBy('date).pivot("Country")
    pivot.agg(sum('Quantity).as("total_qty"), avg('Quantity).alias("avg_qty")).show()

    // creates one Row per date  with Columns Australia_total_qty | Australia_avg_qty | France_total_qty | France_avg_qty |....

    // Like so:
    //  +----------+-------------------+-----------------+
    //  |      date|Australia_total_qty|Australia_avg_qty| ...
    //  +----------+-------------------+-----------------+
    //  |2010-12-01|                107|7.642857142857143| ...
    //  |2010-12-02|               null|             null| ...
    //  +----------+-------------------+-----------------+




  }




}
