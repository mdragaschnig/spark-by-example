package org.apache.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.types.{DataType, UDTRegistration, UserDefinedType}


class ComplicatedObject(var s:String) {
  override def toString: String = s"ComplicatedObject($s)"
}

// Note that this MUST be defined in the package org.apache.spark
// ==> This might stop working in future releases (the API is private)

class ComplicatedObjectUDT extends UserDefinedType[ComplicatedObject] {
  //val kryo = new Kryo()

  override def sqlType: DataType = org.apache.spark.sql.types.BinaryType

  override def serialize(obj: ComplicatedObject): Any = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(obj.s)
    bos.toByteArray
  }

  override def deserialize(datum: Any): ComplicatedObject = {
    val bis = new ByteArrayInputStream(datum.asInstanceOf[Array[Byte]])
    val ois = new ObjectInputStream(bis)
    val s = ois.readObject().asInstanceOf[String]
    new ComplicatedObject("restored: " + s)
  }

  override def userClass: Class[ComplicatedObject] = classOf[ComplicatedObject]

}

object UserDefinedTypeExample {

  case class Data(var x: Int, var co:ComplicatedObject)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Experiments").getOrCreate()
    import spark.implicits._

    // this line can only be called from a package inside org.apache.spark
    UDTRegistration.register(classOf[ComplicatedObject].getName, classOf[ComplicatedObjectUDT].getName)

    implicit val dataEncoder = Encoders.product[Data]

    val myData = Seq(Data(0, new ComplicatedObject("zero")), Data(1, new ComplicatedObject("one")), Data(2, new ComplicatedObject("two")))

    val ds: Dataset[Data] = spark.createDataset(myData)
    ds.show(false)
  }


}
