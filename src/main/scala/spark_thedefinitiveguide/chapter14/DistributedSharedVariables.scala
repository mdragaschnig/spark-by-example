package spark_thedefinitiveguide.chapter14

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

object DistributedSharedVariables {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("DSV_Experiments").getOrCreate()
    import spark.implicits._

    val words = spark.sparkContext.parallelize("Spark The Definitive Guide : Big Data Processing Made Simple".split(" "), 2)

    //
    // Broadcast Variables
    //
    // ... are shared, immutable variables that will be cached on every worker.
    // they are sent to each machine and deserialized once. They must fit into memory.
    // (for example a large lookup table)

    val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200, "Big" -> -300, "Simple" -> 100)
    val suppBroadcast = spark.sparkContext.broadcast(supplementalData)  // broadcast data must be serializable

    // contents of the broadcast can be accessed with .value
    words.map(word => (word, suppBroadcast.value.getOrElse(word, 0))).sortBy(_._2).collect()


    //
    // Accumulators
    //

    // .. provide a mutable variable that a Spark cluster can safely update ona per-row basis

    val flights = spark.read.option("header", "true").option("inferSchema","true").csv("resources/data/flight-data/*.csv")

    // for example to count all flights to/from china with an accumulator:
    val accChina = spark.sparkContext.longAccumulator("china")
    // or:
    //val accChina = new LongAccumulator
    //spark.sparkContext.register(accChina, "china")

    // use the accumulator
    def accChinaFunc(flight:Row) = {
      val destination = flight.getAs[String]("DEST_COUNTRY_NAME")
      val origin = flight.getAs[String]("ORIGIN_COUNTRY_NAME")
      if(destination=="China" ||origin=="China") {
        accChina.add(flight.getAs[Int]("count"))
      }
    }

    flights.foreach(accChinaFunc(_))
    print(accChina.value)

    //
    // Custom accumulators

    // to define a custom accumulator implement AccumulatorV2 like so
    class EvenAccumulator(private var count:Int) extends AccumulatorV2[Int, Int] {
      override def isZero: Boolean = count==0
      override def copy() = new EvenAccumulator(this.count)
      override def reset() {this.count=0}
      override def add(v: Int) {
        if(v % 2 == 0) { // accumulate only the even values
          this.count += v
        }
      }
      override def merge(other: AccumulatorV2[Int, Int]) {this.count += other.value}
      override def value: Int = this.count
    }

    val myAcc = new EvenAccumulator(0)
    spark.sparkContext.register(myAcc, "evenAcc")

    myAcc.value
    flights.foreach(row => myAcc.add(row.getAs[Int]("count")))
    myAcc.value

  }


}
