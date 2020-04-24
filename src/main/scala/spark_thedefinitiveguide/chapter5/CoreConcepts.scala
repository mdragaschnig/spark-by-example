package spark_thedefinitiveguide.chapter5

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType, StructField, StructType}

object CoreConcepts {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("Experiments").getOrCreate()
    val df = spark.read.format("csv").option("header", true).option("inferSchema", true).load("resources/data/flight-data/2010-summary.csv")

    // DataFrames have a schema
    df.schema

    //
    // Columns

    // columns in spark are similar to columns in a spreadsheet. Column values can refer to columns from the  input data,
    // or they can be calculated during execution
    df.columns

    // Columns are "resolved" (and possibly calculated) during execution
    col("count") // spark will try to resolve this during execution
    df.col("count")  // we have told spark how to resolve the column

    // Columns are really just wrappers for Expressions
    // Expression:
    //             - basically is function on a Row of a DataFrame (in: one Row of a DF,  out: one value)
    //               it has a method:
    //                                def eval(input: InternalRow = EmptyRow): Any
    //             - an Expression is "an executable node in a Catalyst tree" (https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-Expression.html )
    //             - Expressions can be nested (and thus complex)
    df.col("count")*5  >= df.col("ORIGIN_COUNTRY_NAME")-3

    //
    // Rows

    // DataFrames consist of Rows. Each Row is a single record. Spark manipulates Rows using Expressions
    val row = df.first()

    // Accessing data in rows can be done by index or name.
    // But Rows do not have a schema, so a type needs to be specified
    row.getAs[String](0)
    row.getAs[Int]("count")

    // (Rows are internally just bytearrays that Spark interprets at runtime)


  }


}
