package encoders

import java.time.Instant

import org.apache.spark.GenericKryoUDT.{GenericKryoUDT, GenericKryoUDTRegistrationHelper}
import org.apache.spark.sql.{Encoders, SparkSession}

object GenericKryoUDTExample {

  //
  // Usage example: assuming you have a case class that you want to encode with Encoders.product
  case class Record(x:Double, y:Double, time:Instant)
  // then you need to define a UDT for Instant

  // You can now do this by extending GenericKryoUDT like so:
  class MyInstantUDT extends GenericKryoUDT[Instant] {
    override def userClass: Class[Instant] = classOf[Instant]
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Experiments").getOrCreate()
    import spark.implicits._

    // register the MyInstantUDT
    GenericKryoUDTRegistrationHelper.register(classOf[Instant], classOf[MyInstantUDT])
    implicit val recordEncoder = Encoders.product[Record] // now we can use Encoders.product

    val recs = Seq(
      Record(1.0, 1.1, Instant.parse("2020-05-07T14:50:00Z")),
      Record(1.0, 1.1, Instant.parse("2020-05-07T14:55:00Z"))
    )

    val ds = spark.createDataset(recs)
    ds.show(true)
  }

}
