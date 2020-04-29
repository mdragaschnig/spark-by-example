package spark_thedefinitiveguide.chapter6


import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions.{col, expr, lit, coalesce, when, udf}
import org.apache.spark.sql.functions.{split, size, array_contains, explode, map}


object DataTypes_Complex {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("Experiments").getOrCreate()
    import spark.implicits._

    var df = spark.read.format("csv").option("header", true).option("inferSchema", true).load("resources/data/retail-data-by-day/2010-12-01.csv")

    //
    // Arrays
    //

    //
    // Create Arrays

    // using lit()
    df.withColumn("Arr", lit(Array(1,2,3))).show()

    // create an array by splitting a string
    df.withColumn("Arr", split('Description, " ")).show()

    // using a UDF
    val createIntArray = udf { size:Int => Array.ofDim[Int](size) }
    df.withColumn("Arr", createIntArray('Quantity)).show()

    //
    // Work with Array
    val df2 = df.withColumn("Arr", split('Description, " "))

    // access elements
    df2.select('Arr(1)).show()
    df2.selectExpr("Arr[1]").show()   // probably less confusing

    // array size
    df2.select(size('Arr)).show()

    // check if array contains element
    df2.select(array_contains('Arr, "WHITE")).show()

    //
    // Exploding an Array

    // (transform Row with Array[N] in to N Rows, each with a different Array element)
    df2.withColumn("array_element", explode('Arr)).show()


    //
    // Maps
    //

    //
    // Create a Map

    // from the data in the DF
    df.withColumn("Map", map('Description, 'InvoiceNo))

    // using a UDF
    val createWordLengthMap = udf { text:String =>  text.split(" ").map( x=> (x, x.length)).toMap }
    val df3 = df2.withColumn("Map", createWordLengthMap('Description))

    //
    // Work with Map

    // size
    df3.select(size('Map)).show()

    // access elements
    df3.select('Map("WHITE")).show()
    df3.selectExpr("Map['WHITE']").show()

    // Exploding (to DF with key/value columns)
    df3.select(explode('Map)).show()

    // Explode & add as columns
    df3.select(expr("*"), explode('Map)).show() // adds key/value columns to dataFrame



  }



}
