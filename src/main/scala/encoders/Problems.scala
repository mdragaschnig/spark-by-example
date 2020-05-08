package encoders

import org.apache.spark.sql.{Encoders, SparkSession}

object Problems {

  class Data(var x:Int) {}

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Experiments").getOrCreate()
    import spark.implicits._

    implicit val dataEncoder = Encoders.kryo[Data]
    val ds = spark.sparkContext.parallelize(1 to 10).map(x => new Data(x)).toDF().as[Data]
    ds.printSchema()
    ds.show() // for some reason we see the binary row data?

  }
}
