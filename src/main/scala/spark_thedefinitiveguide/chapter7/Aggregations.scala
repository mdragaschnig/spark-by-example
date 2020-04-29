package spark_thedefinitiveguide.chapter7

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions.{col, expr, lit, udf}
import org.apache.spark.sql.functions.{approx_count_distinct, avg, count, countDistinct, first, kurtosis, last, max, min, skewness, sum, var_samp}
import org.apache.spark.sql.functions.{collect_list, collect_set, to_date, rank, dense_rank, row_number}

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
    df.select(var_samp('Quantity)).show()  // or varPop for population Variance

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

    // (it's possible use multiple different Window definitions in the same select)
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




  }




}
