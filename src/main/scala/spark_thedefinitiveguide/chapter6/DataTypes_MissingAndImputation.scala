package spark_thedefinitiveguide.chapter6

import java.sql.Timestamp
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions.{col, expr, lit, coalesce, when, udf, asc_nulls_first}

object DataTypes_MissingAndImputation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("Experiments").getOrCreate()
    import spark.implicits._

    var df = spark.read.format("csv").option("header", true).option("inferSchema", true).load("resources/data/retail-data-by-day/2010-12-01.csv")
    df = df.withColumn("StockCode2", when('Quantity < 6, 'StockCode).otherwise(null))


    //
    // Removing Data
    //

    // if Column has Value, set to null
    df.selectExpr("nullif(StockCode2, 22752)").show()

    // drop nulls
    df.na.drop("any").show()  // remove row if any column has null
    df.na.drop("any", Seq("StockCode2")).show()  // remove row if any of the provided columns has null

    df.na.drop("all").show()  // remove row if ALL columns are null
    df.na.drop("all", Seq("StockCode", "StockCode2")).show()    // remove row if all provided columns have null


    //
    // Imputing Missing Data
    //

    // Coalesce columns (take value from first non-null column)
    df.select(coalesce('StockCode2, 'Quantity)).show()
    df.select(coalesce('StockCode2, lit("missing"))).show()

    // fill
    df.na.fill("all null values (in string columns) become this string").show()

    val df2 = df.withColumn("Quantity2", when('Quantity < 6, 'Quantity).otherwise(null)).
              withColumn("UnitPrice2", when('Quantity < 6, 'UnitPrice).otherwise(null)).
              withColumn("InvoiceDate2", when('Quantity < 6, 'InvoiceDate).otherwise(null))

    df2.na.fill(5:Int).show()  // applied to all NUMERIC columns
    df2.na.fill(5.5:Double).show()  // also applied to all numeric columns (decimal portion truncated for int columns)

    // .. unfortunately this  works only for String,Int,Long,Double,Float,Boolean. (see https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameNaFunctions.html )
    // It does not work for Timestamps.
    // Workaround - impute a Timestamp column like so:
    df2.withColumn("InvoiceDate2", when('InvoiceDate2.isNotNull, 'InvoiceDate2).otherwise(Timestamp.valueOf("2020-02-02 14:30:00"))).show()

    // fill specific columns only
    df2.na.fill(5.5, Seq("UnitPrice2")).show()
    df2.na.fill(Map("StockCode2"->"missing", "UnitPrice2"->7)).show()

    //
    // Imputing Non-Null Data
    //

    // simple value -> value replacement
    df2.na.replace("StockCode2", Map("22752"-> "nope")).show()

    // for complex replacements UDFs can be used
    val truncateStockCode = udf { code:String =>  if(code==null) "xxx" else if(code.length>3) code.substring(0,3)+"(trunc)" else code}
    df2.withColumn("StockCode2", truncateStockCode('StockCode2)).show()


    //
    // Ordering (with nulls)
    //
    df2.sort(asc_nulls_first("StockCode2")).show()

  }

}
