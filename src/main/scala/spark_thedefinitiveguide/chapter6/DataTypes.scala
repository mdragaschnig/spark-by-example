package spark_thedefinitiveguide.chapter6

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions.{col, corr, expr, lit, monotonically_increasing_id, pow, round, struct, udf, when}

// string functions
import org.apache.spark.sql.functions.{initcap, upper, lower,trim, ltrim, lpad, rtrim, rpad, regexp_replace, regexp_extract, length}

object DataTypes {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("Experiments").getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", true).option("inferSchema", true).load("resources/data/retail-data-by-day/2010-12-01.csv")

    //
    // Constants
    //  native Scala types need to be converted to Spark types using lit()
    df.withColumn("one", lit(1)).show(10)

    //
    // Booleans
    df.withColumn("pricey_item", col("UnitPrice") > 5).show()
    df.where("UnitPrice > 5").select("InvoiceNo", "Description").show()
    df.where(col("InvoiceNo")==="536381").show()  // remember to use ===  and =!=   (instead of == and !=)

    // conditions can be put into variables
    val stockCodeIsDot = col("StockCode") === "DOT"
    df.where(stockCodeIsDot).show(10)

    // dealing with nulls
    val df_withNulls = df.withColumn("StockCode2", when('Quantity < 6, 'StockCode).otherwise(null))
    df_withNulls.where('StockCode2.eqNullSafe("DOT")).show()  // however i can not get this to fail without the nullSafe check
    df_withNulls.where('StockCode2.equalTo("DOT")).show()   // this should fail but doesn't ?
    df_withNulls.where('StockCode2.isNull).show()  // this definitely returns some rows

    //
    // Numbers

    // calculating values in scala
    val newQty = pow('Quantity * 'UnitPrice, 2) + 5
    df.select('CustomerId, 'Quantity.as("oldQuantity"), newQty.as("newQuantity")).show(2)
    // .. or in SQL
    df.selectExpr("CustomerId", "(POWER(Quantity * UnitPrice, 2.0) + 5) as newQuantity").show()

    // rounding
    df.select('UnitPrice.as("orig"), round('UnitPrice, 1).as("rounded")).show() // round to 1 decimal place (rounds up)

    // show data summary
    df.describe().show()

    // correlation
    df.stat.corr("Quantity", "UnitPrice")
    df.select(corr('Quantity, 'UnitPrice)).show()

    // quantiles (approximate)
    df.stat.approxQuantile("UnitPrice", Array(0.25, 0.5, 0.75), 0.05)

    // cross-tabulation
    df.stat.crosstab("StockCode", "UnitPrice").show()

    // add a unique ID
    df.withColumn("id", monotonically_increasing_id()).show()

    //
    // Strings

    //  case transforms
    df.select('Description, upper('Description), lower('Description), initcap('Description)).show(false)

    // padding
    df.select('Description, trim('Description), lpad(lit("Test"), 10, " ")).show(false)

    // select where contains
    df.where('Description.contains("WHITE")).show()

    // regex replace
    df.select(regexp_replace('Description, "WHITE", "white")).show()

    // regex extract (extract names of white items)
    val items = df.select('Description, regexp_extract('Description,"WHITE (\\w+)" , 1).as("Extracted"))
    items.where(length('Extracted)>0).show()

    // generate multiple columns
    val colors = Seq("black", "white", "red", "green", "blue")
    val selectedCols = colors.map(color => 'Description.contains(color.toUpperCase).alias(s"is_$color")) :+ expr("*")
    df.select(selectedCols:_*).show()   // :_* unpacks the values of the seq and uses them as method varargs


  }


}
