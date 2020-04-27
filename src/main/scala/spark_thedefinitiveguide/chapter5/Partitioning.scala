package spark_thedefinitiveguide.chapter5


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, expr}

object Partitioning {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("Experiments").getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv").option("header", true).option("inferSchema", true).load("resources/data/flight-data/2010-summary.csv")

    //
    // Partitioning

    // the data in the DataFrame / RDD is usually held in Partitions which are
    // distributed over the worker Nodes of the cluster
    // (the driver usually only holds the state and coordinates calculations)

    // show current number of partitions
    df.rdd.getNumPartitions

    // re-partitioning (note: re-partitioning will incur a full shuffle!)
    val df_5pt = df.repartition(5)  // change number of partitions

    // repartition can be performed on complex expressions
    val df_cnt  = df.repartition(col("count") % 10)

    // note that this will NOT create 10 Partitions. It will create $spark.sql.shuffle.partitions Partitions
    // (most likely 200, but most of them will be EMPTY)

    // BTW: this can easily be verified using :
        val nonEmptyPartitions = spark.sparkContext.longAccumulator("nonEmptyPartitions")
        df_cnt.rdd.foreachPartition(partition =>
          if (partition.length > 0) nonEmptyPartitions.add(1)
        )
        print(nonEmptyPartitions.value)

    // numPartitions can also be explicitly set for complex expressions
    df.repartition(10, col("count")%10)

    // Coalescing (Merging Partitions)
    val df_cnt2 = df_cnt.coalesce(10)  // reduce from 200 to 10 partitions. this will NOT incur a full shuffle


    //
    // Collecting Data on the Driver

    // the data in the DataFrame / RDD is distributed over all Nodes in the cluster
    // (the driver usually only holds the state and coordinates calculations)
    // if data is needed on the driver, it needs to be collected (be careful to avoid OutOfMemory problems)
    df.show(5)
    val someItems: Array[Row] = df.take(5)
    val allItems: Array[Row] = df.collect()  // possible OutOfMemory here!

    // to iteratively process the results on the driver:
    import scala.collection.JavaConverters._   // required  for .asScala (which converts the clumsy java iterator to scala)
    df.toLocalIterator().asScala.foreach(row => println(row))
  }
}
