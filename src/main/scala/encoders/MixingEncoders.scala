package encoders

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object MixingEncoders {

  class ComplicatedObject(var s:String) {
    def getX() = s
    def setX(v:String) {this.s=v}
  }

  case class Data(var x: Int, var co:ComplicatedObject)

  def main(args: Array[String]): Unit = {
    // Unfortunately, mixing Encoders does not work.
    // It seems, the Encoder of the outmost class in the Dataset defines, how Spark tries to serialize the ENTIRE structure.

    val spark = SparkSession.builder.master("local[*]").appName("Experiments").getOrCreate()
    import spark.implicits._

    implicit val complicatedObjectEncoder = Encoders.kryo[ComplicatedObject]  // we would like to serialize ComplicatedObject with kryo
    implicit val dataEncoder = Encoders.product[Data]  // but the enclosing Data object with Encoders.Product

    // this does not work (it fails at runtime) because it's not possible to mix encoders like that
    // see UserDefinedTypeExample.scala and InstantAsUDT.scala for a Workaround

    // Some helpful general info:
    // https://stackoverflow.com/questions/36648128/how-to-store-custom-objects-in-dataset/39442829#39442829
    // https://stackoverflow.com/questions/36144618/spark-kryo-register-a-custom-serializer

    // lines are never reached
    val myData = Seq(Data(0, new ComplicatedObject("zero")), Data(1, new ComplicatedObject("one")), Data(2, new ComplicatedObject("two")))
    val ds: Dataset[Data] = spark.createDataset(myData)
    ds.show(false)

  }


}