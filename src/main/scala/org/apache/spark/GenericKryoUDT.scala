package org.apache.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.time.Instant

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.sql.types.{DataType, UDTRegistration, UserDefinedType}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy
import org.objenesis.strategy.StdInstantiatorStrategy

object GenericKryoUDT {

  // If you want to create a UDT that is serialized with Kryo you can save some typing by using this abstract class as a base:

  // --- This must be in package org.apache.spark
  abstract class GenericKryoUDT[T>:Null]  extends UserDefinedType[T] {
    override def sqlType: DataType = org.apache.spark.sql.types.BinaryType

    override def serialize(obj: T): Any = {
      val out = new Output(1024, -1);
      new Kryo().writeObject(out, obj)
      out.getBuffer
    }

    override def deserialize(datum: Any): T = {
      val kryo = new Kryo()
      // this is necessary for classes that don't have a zero-argument constructor
      kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy))
      kryo.readObject(new Input(datum.asInstanceOf[Array[Byte]]), userClass)
    }
  }

  // Convenience Function to register UDTs from client code
  object GenericKryoUDTRegistrationHelper {
    def register(dataCls:Class[_],  udtCls:Class[_]): Unit = {
      UDTRegistration.register(dataCls.getName, udtCls.getName)
    }
  }
  // -- END package org.apache.spark


  // see  encoders.GenericKryoUDTExample for a usage example
}
