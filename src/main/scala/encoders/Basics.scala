package encoders

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext



object Basics {

  def main(args: Array[String]): Unit = {
    // from : https://towardsdatascience.com/apache-spark-dataset-encoders-demystified-4a3026900d63


    // Central to the concept of Dataset is an Encoder framework which provides Dataset with storage and execution efficiency gains
    // A Dataset always needs a corresponding Encoder of the same type
    // An encoder encodes / decodes between a Java object or a data record and a spark-internal binary format (backed by raw memory)

    // Theis provides the following benefits:
    //    - Storage efficiency
    //    - Query efficiency (binary format is structured -> can deserialize a single field required for a query instead of the entire object)
    //    - Shuffle efficiency (encoded format is smaller than e.g. Java Serialization -> less data needs to be transferred)

    //
    // Encoders
    //

    // Spark provides an "Encoder" interface and a generic implementation "ExpressionEncoder"

    //
    // Expression Encoder
    // see https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-ExpressionEncoder.html
    // ExpressionEncoder[T] is a generic Encoder which uses serializer expressions to encode a JVM object to an internal binary row format ("InternalRow")
    // ExpressionEncoders can be created using the factor methods in "Encoders"

    //
    // Encoders.product() - recommended for Scala's product types (tuples, case classes).

    //
    // Encoders.bean() - recommended for Java Beans which consist of primitives
    // uses the getX() / setX() methods of an object

    class MyClass() {
      var someValue:Int=12
      var otherValue:Int=100
      def getSomeValue() = someValue
      def setSomeValue(v:Int) {this.someValue=v}
      def getOtherValue() = otherValue
      def setOtherValue(v:Int) {this.otherValue=v}
    }

    val myBeanEnc: ExpressionEncoder[MyClass] = Encoders.bean(classOf[MyClass]).asInstanceOf[ExpressionEncoder[MyClass]]
    // interestingly the Encoder trait itself is almost empty and it seems that every Encoder is cast to ExpressionEncoder inside the DataSet anyway.
    // (not sure whats the reasoning behind that)

    val obj = new MyClass()
    val row: InternalRow = myBeanEnc.toRow(obj)
    // the row has one field per member variable of MyClass
    row.numFields

    // can access individual variables without deserializing the entire row
    row.getInt(0)
    row.getInt(1)

    // In Java Bean based ExpressionEncoders, the bean object is mapped to the binary format by just keeping its fields in the binary format
    //  -> storage efficiency
    //  -> faster querying of individual fields
    // For Datatsets composed of complex datatypes, one should always try to construct datatype as Java beans consisting of fields
    // for which the Encoders Factory supports ExpressionEncoders

    // In serialization based ExpressionEncoders (kryo, javaSerialization), the whole object is serialized.
    // the serialized byte string is kept as the only single field in the encoded binary format
    // ==> these Encoders lack storage efficiency and one cannot directly query particular fields of the object from the encoded binary format


    //
    // Encoders.kryo()  - serializes entire object to binary. Preferred over javaSerialization (because faster) but has similar drawbacks (no field access)

    val myKryoEnc: ExpressionEncoder[MyClass] = Encoders.kryo[MyClass].asInstanceOf[ExpressionEncoder[MyClass]]
    val kryoRow: InternalRow = myKryoEnc.toRow(obj)
    kryoRow.numFields // this will return 1 (instead of 2), because the entire object is stored as a single field

    // accessing fields by index does not yield usable values
    kryoRow.getInt(0)
    kryoRow.getInt(1)
  }

}
