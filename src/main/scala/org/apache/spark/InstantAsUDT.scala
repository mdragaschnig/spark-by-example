package org.apache.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.time.Instant

import org.apache.spark.sql.types.{DataType, UDTRegistration, UserDefinedType}
import org.apache.spark.sql.{Encoders, SparkSession}

object InstantAsUDT {

  // assuming you have a harmless looking case class like this
  case class Record(x:Double, y:Double, time:Instant)

  // If want to use Record in a Dataset[Record] you would like to define Encoders.product[Record] to gain maximum performance and convenience
  // But this will FAIL (because Instant is neither a scala Product nor a primitive type and Encoders.product thus does not know how to deal with it)
  // Also note that simply defining Encoders.kryo[Instant] will not help in this case (because the Encoder of the outer-most class defines how the entire structure is serialized)

  // One worakround is to define Encoders.kryo[Record]. This would WORK, but it transforms the entire Record into a binary blob
  // (which is quite bad for filters, joins and performance in general)

  // One way out of this dilemma is to define&register a User Defined Type (which essentially implements the transforms: Instant -> binary -> Instant)

  // WARNING: the one Downside of this approach is that it uses a private API of Spark (which might no longer work in newer versions)
  // ==> the following section thus needs to be in the org.apache.spark package

  // -----------------------------------
  // BEGIN (code in package org.apache.spark)
  // -----------------------------------

  class InstantUDT extends UserDefinedType[Instant] { // the UserDefinedType class is only visible from the org.apache.spark package!
    override def sqlType: DataType = org.apache.spark.sql.types.BinaryType

    override def serialize(obj: Instant): Any = {
      val bos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(bos)
      oos.writeLong(obj.toEpochMilli)
      oos.flush()
      bos.toByteArray
    }

    override def deserialize(datum: Any): Instant = {
      val bis = new ByteArrayInputStream(datum.asInstanceOf[Array[Byte]])
      val ois = new ObjectInputStream(bis)
      val millis = ois.readLong()
      Instant.ofEpochMilli(millis)
    }

    override def userClass: Class[Instant] = classOf[Instant]
  }

  // we also need to add a method to register the UDT from our main()
  object InstantUDT {
    def register(): Unit = {
      // this line can only be called from a package inside org.apache.spark
      UDTRegistration.register(classOf[Instant].getName, classOf[InstantUDT].getName)
    }
  }

  // -----------------------------------
  // END  (code in package org.apache.spark)
  // -----------------------------------

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Experiments").getOrCreate()
    import spark.implicits._

    InstantUDT.register() // need register the UDF

    implicit val recordEncoder = Encoders.product[Record] // now we can use Encoders.product

    val recs = Seq(
      Record(1.0, 1.1, Instant.parse("2020-05-07T14:50:00Z")),
      Record(1.0, 1.1, Instant.parse("2020-05-07T14:50:00Z"))
    )

    val ds = spark.createDataset(recs)
    ds.show(true)
  }
}
