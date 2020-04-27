package spark_thedefinitiveguide.chapter5

import org.apache.spark.sql.functions.{struct, udf, col, when}
import org.apache.spark.sql.{Row, SparkSession}

object Transformations_Ext {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("Experiments").getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", true).option("inferSchema", true).load("resources/data/retail-data-by-day/2010-12-01.csv")

    //
    // Changing column values

    // ... is NOT SUPPORTED (RDDs/DataFrames/DataSets are basically immutable)
    // ==> new columns need to be created when a value has to be changed

    //
    // Example: we want to add a 10% discount for quantities > 5

    // using when()
    df.withColumn("price_discount", when('Quantity < 6, 'UnitPrice).otherwise('UnitPrice*0.9)).show()

    // Using a UDF
    val discountFunc = udf { (quantity:Int, price:Double) =>
      if(quantity<6) price else 0.9*price
    }
    df.withColumn("price_discount", discountFunc('Quantity, 'UnitPrice)).show()

    // it is also possible to pass the entire Row to the UDF
    // WARNING: this will probably negatively impact performance and should thus BE AVOIDED where possible
    val discountFunc2 = udf { r:Row =>
      val quantity = r.getAs[Int]("Quantity")
      val price = r.getAs[Double]("UnitPrice")
      if(quantity<6) price else 0.9*price
    }
    df.withColumn("price_discount", discountFunc2(struct("*"))).show()

  }

}
