package spark_thedefinitiveguide.chapter6

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, expr, lit, udf, when, struct}


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




  }


}
