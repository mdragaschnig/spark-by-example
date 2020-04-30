package spark_thedefinitiveguide.chapter7

import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.{Column, Dataset, Encoder, Encoders, RelationalGroupedDataset, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, expr, lit, udf}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

object UDAFs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Experiments").getOrCreate()
    import spark.implicits._

    var df = spark.read.format("csv").option("header", true).option("inferSchema", true).load("resources/data/retail-data-by-day/*.csv")
    df.cache()


    // Spark has two different interfaces for user defined Aggregate functions.
    // Both of them are stateless and store the intermediate results of an ongoing aggregation in an external buffer.
    // Implementing an Aggregator means defining methods for initializing/extending/merge buffer states
    // (as well as a method to calculate the final result from the buffer).

    // The two UDAF interfaces are:

    // UserDefinedAggregateFunction:
    //
    //    - operates on a set of Columns
    //    - user needs to define StructTypes for Input/Buffer/Result
    //    - working with the Buffer feels clumsy and error-prone (updates must be performed by field-index. Gets need explicit casts)

    // Aggregator:
    //
    //    - Aggregator takes a complete Row
    //    - it is probably intended for the "strongly" typed DataSet API
    //    - working with the Input/Buffer is less clumsy (can use field names or fields)
    //    - needs explicit Encoders for Buffer/Result (however they are usually trivial to define)

    // Main Differences:
    //    Aggregator API is more user friendly
    //    Aggregator is perhaps less flexible (seems it can only be applied to Rows, not to individual Columns?)
    //    Aggregator MIGHT be slower (it possibly needs to deserialize the entire Row?)
    //


    //
    // UserDefinedAggregateFunction
    //

    // Define your function by extending UserDefinedAggregateFunction
    class MyAvg extends UserDefinedAggregateFunction {
      // type of the input columns
      override def inputSchema: StructType = StructType(Seq(StructField("value", DoubleType)))

      // the Aggregation state is stored in the bufferSchema
      override def bufferSchema: StructType = StructType(Seq(StructField("sum", DoubleType), StructField("count", LongType)))

      // type of the aggregation result
      override def dataType: DataType = DoubleType
      override def deterministic: Boolean = true

      override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0)=0d  // careful with the data types here, they must match perfectly otherwise you get Exceptions
        buffer(1)=0l
      }

      override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getAs[Double](0) + input.getAs[Double](0) // sum
        buffer(1) = buffer.getAs[Long](1)+1 // count
      }

      override def merge(buffer: MutableAggregationBuffer, other: Row): Unit = {
        // merge other into buffer
        buffer(0)  = buffer.getAs[Double](0) + other.getAs[Double](0)
        buffer(1)  = buffer.getAs[Long](1) + other.getAs[Long](1)
      }

      // generate final result
      override def evaluate(buffer: Row): Any = buffer.getAs[Double](0) / buffer.getAs[Long](1)
    }

    // using the function
    val myAvg = new MyAvg()  // remember the function is stateless, so one instance is enough
    df.groupBy('Country).agg(avg('Quantity), myAvg('Quantity)).show() // use myavg like you would use any other aggregate function


    //
    // Aggregator (with DataFrame)
    //

    // As in the example above, the Aggregator itself is stateless
    // the AggregationBuffer is used to store the intermediate result of a Group during Aggregation
    case class AggregationBuffer(var sum:Double,var count:Long)

    class MyAvg2 extends Aggregator[Row, AggregationBuffer, Double] {
      // it seems, when working with DataFrames the InputType MUST be 'Row'

      override def bufferEncoder: Encoder[AggregationBuffer] = Encoders.product[AggregationBuffer]
      override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

      override def zero: AggregationBuffer = AggregationBuffer(0,0)

      override def reduce(b: AggregationBuffer, row:Row): AggregationBuffer = {
        b.sum += row.getAs[Int]("Quantity")
        b.count += 1
        b
      }

      override def merge(b1: AggregationBuffer, b2: AggregationBuffer) =  AggregationBuffer(b1.sum + b2.sum, b1.count + b2.count)
      override def finish(s: AggregationBuffer): Double = s.sum / s.count
    }

    val myAvg2 = new MyAvg2().toColumn   // Aggregator needs to be converted to a Column so we can use it below
    df.groupBy('Country).agg(avg('Quantity), myAvg2).show()   // Note: I have not found a way to specify a column here,
                                                                    //       seems, the UDAF always gets the entire Row as Input

    //
    // Aggregator (with Dataset)
    //

    // the main difference to above is that the input type can here be a case class instead of the generic Row

    case class Input(country:String, quantity:Int)

    class MyAvg3 extends Aggregator[Input, AggregationBuffer, Double] {
      override def bufferEncoder: Encoder[AggregationBuffer] = Encoders.product[AggregationBuffer]
      override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

      override def zero: AggregationBuffer = AggregationBuffer(0,0)

      override def reduce(b: AggregationBuffer, v:Input): AggregationBuffer = {
        b.sum += v.quantity
        b.count += 1
        b
      }

      override def merge(b1: AggregationBuffer, b2: AggregationBuffer) =  AggregationBuffer(b1.sum + b2.sum, b1.count + b2.count)
      override def finish(s: AggregationBuffer): Double = s.sum / s.count
    }

    val ds: Dataset[Input] = df.select('Country, 'Quantity).as[Input]

    // note that we need to use groupByKey here (instead of groupBy)
    // ... otherwise our Aggregator will fail because we would be back in the "DataFrame world"
    // (we would operate on a RelationalGroupedDataset again and the Aggregator would get a GenericRow instead of an instance of "Input")
    ds.groupByKey(x=>x.country).agg(
        avg('Quantity).as[Double],  // need to explicitly specify a type here (because Dataset!)
        new MyAvg3().toColumn       // our aggregator (again need to convert it to Column to have it accepted inside an agg() )
    ).show()

  }

}
