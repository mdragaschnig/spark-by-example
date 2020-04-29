package spark_thedefinitiveguide.chapter6

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.{col, expr, lit, udf}

object UDFs {



  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("Experiments").getOrCreate()
    import spark.implicits._

    var df = spark.read.format("csv").option("header", true).option("inferSchema", true).load("resources/data/retail-data-by-day/2010-12-01.csv")

    // Define a UDF
    val powerUdf = udf{ number:Double => number*number*number}

    // Use UDF
    df.select(powerUdf('Quantity)).show()

    // Register a normal scala function as UDF
    def power3(number:Double):Double = number * number * number
    val power3Udf = udf(power3(_:Double):Double) // explicit types are necessary here!
    df.select(power3Udf('Quantity)).show()

    // Register a function in spark SQL
    spark.udf.register("power3", power3(_:Double):Double)
    df.selectExpr("power3(Quantity)").show()  // can now use it in SQL like expressions

  }


  }
