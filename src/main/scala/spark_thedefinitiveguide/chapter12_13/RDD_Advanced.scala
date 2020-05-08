package spark_thedefinitiveguide.chapter12_13

import java.lang
import java.time.Instant

import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}

import scala.util.Random

class RDD_Advanced {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Experiments").getOrCreate()
    import spark.implicits._

    val words: RDD[String] = spark.sparkContext.parallelize("Spark the Definitive Guide: Big Data Processing Made Simple".split(" "), 2)

    // create RDD of Key-Value Pairs
    val kv: RDD[(String, String)] = words.keyBy(word => word.toLowerCase.charAt(0).toString)  // key: firstLetter, value: word

    // get keys/values
    kv.keys.collect()
    kv.values.collect()

    // lookup values by key
    kv.lookup("s")

    //
    // manipulate values
    kv.mapValues(word => word.toUpperCase).collect()

    // or
    kv.flatMapValues(word => word.toUpperCase().toSeq).collect()

    //
    // Aggregation
    //

    // for these operations you need an RDD[Pair]. The first element of the pair is used as the key, the second das the value!

    val nums = spark.sparkContext.parallelize(1.to(30), 5)
    val chars = words.flatMap(word => word.toLowerCase().toSeq)
    val KVcharacters: RDD[(Char, Int)] = chars.map(letter => (letter, 1))

    // countByKey
    KVcharacters.countByKey()

    // groupByKey() : puts all records with the same key in the same partition and provides an Iterable of all values for each key
    //
    // Usually not a good solution:
    //    - can be quite expensive (usually needs a shuffle)
    //    - can run out of memory  (in the current implementation the whole value Iterator will be put into memory)
    val grouped1: RDD[(Char, Iterable[Int])] = KVcharacters.groupByKey()  // shuffle !
    grouped1.map(row => (row._1, row._2.reduce( (v1,v2) => v1+v2))).collect()  // can run out of memory here

    // reduceByKey()  :  groups by key and reduces in a single operation
    //
    //  - usually more efficient (no shuffle, performs most work on the existing partitions, then performs a final reduce to merge these partial results)
    //  - elements in a group do NOT have a guaranteed ordering (the elements of a group can be in separate partitions and even on separate workers!)
    KVcharacters.reduceByKey((v1, v2) => v1+v2).collect()


    // aggregate()

    // provides more control (and needs more information)
    //  - zero value
    //  - function for aggregating values within a partition
    //  - function for combining results from different partitions
    nums.aggregate(0)( (v1,v2)=>math.max(v1,v2), (max1, max2)=>max1+max2) // sum of (max value within each partition)

    // note that the final aggregation (merge of all partition results) will be performed on the driver
    // --> the DRIVER CAN RUN OUT OF MEMORY if the intermediate results are large

    // treeAggregate()  is similar to aggregate() but pushes down some of the subaggregations to the workers
    //                  multiple levels of aggregations are a bit slower but can help if the driver runs out of memory

    val depth = 3
    nums.treeAggregate(0)( (v1,v2)=>math.max(v1,v2), (max1, max2)=>max1+max2)


    // aggregateByKey()
    // similar to aggregate but instead of aggregating by partition it aggregates by key (first element in Pair)

    KVcharacters.aggregateByKey(0)(_+_, _+_).collect()


    // foldByKey()
    // similar to aggregateByKey. Can be used if the operations for merging values and partitions are the same
    KVcharacters.foldByKey(0)(_+_).collect()

    // combineByKey()
    // if more flexibility is required than aggregate/foldByKey can provide. It uses a "combiner" data structure
    // that accumulates the values.
    // The following functions are required:
    //   - create a combiner
    //   - merge a new value into the combiner
    //   - merge two combiners

    val res: RDD[(Char, List[Int])] = KVcharacters.combineByKey(
      /*createCombiner=*/ (v:Int) => List(v),
      /*mergeValue=*/     (list:List[Int], value:Int) => value :: list,
      /*mergeCombiners=*/ (list1:List[Int], list2:List[Int]) => list1 ::: list2,
      /*outputPartitions=*/ 10
    ) // res contains (key, completeCombiner) tuples


    //
    // CO-Groups

    val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
    val rng = new Random()
    val charRDD: RDD[(Char, Double)] = distinctChars.map(c => (c, rng.nextDouble())).cache()
    val charRDD2: RDD[(Char, Double)] = distinctChars.map(c => (c, rng.nextDouble())).cache()

    //  group together records of charRDD with records of charRDD2 (group by value of first element in the pair)
    charRDD.cogroup(charRDD2).collect()


    //
    // Joins
    val keyedChars = distinctChars.map(c => (c, rng.nextDouble())).cache()
    val numPartitions = 10
    KVcharacters.join(keyedChars, numPartitions).collect()
    // lots of other join functions exist:  fullOuterJoin, leftOuterJoin, rightOuterJoin, ...


    //
    // Zips

    // combine two RDDs with the same length (and same number of partitions)
    val numRange = spark.sparkContext.parallelize(1 to 9, 2)
    words.zip(numRange).collect() // this only works when length and number of partitions is equal


    //
    // Controlling Partitions
    //

    // coalesce - collapses the data on a single worker in order to avoid shuffles during repartitioning
    //
    words.getNumPartitions
    words.coalesce(1).getNumPartitions

    // repartition - allows changing number of partitions up or down
    words.repartition(10)  // performs a shuffle across all nodes


    // repartitionAndSortWithinPartitions
    // KVcharacters.repartitionAndSortWithinPartitions(partitioner)

    //
    // Custom Partitioning

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("resources/data/retail-data-by-day/*.csv")
    val rdd = df.repartition(10).rdd
    val keyedRdd: RDD[(Double, Row)] = rdd.keyBy(row => row(6).asInstanceOf[Double])  // key=customerId, value=full_record

    // Builtin Partitioners:

    keyedRdd.partitionBy(new HashPartitioner(10)).take(10)
    keyedRdd.partitionBy(new RangePartitioner(10,keyedRdd, ascending=true)).take(10)

    // Custom Partitioner

    class ExamplePartitioner extends Partitioner {
      override def numPartitions: Int = 3

      override def getPartition(key: Any): Int = {
        val customerId = key.asInstanceOf[Double].toInt
        if(customerId==17850) { // customer 17850 gets his own partition
          0
        } else { // everything else is randomly distributed between two partitions
          new Random().nextInt(2)+1
        }
      }
    }

    val parts = keyedRdd.partitionBy(new ExamplePartitioner()).glom().collect()

    //
    // Serialization
    //

    class SomeClass(var value:Int) {}
    spark.sparkContext.parallelize(1 to 10).map(n => new SomeClass(n)).collect() // this will fail because SomeClass is not Serializable

    class SomeClass2(var value:Int) extends Serializable {}
    spark.sparkContext.parallelize(1 to 10).map(n => new SomeClass2(n)).collect() // this works

    case class SomeClass3(value:Int)
    spark.sparkContext.parallelize(1 to 10).map(n => SomeClass3(n)).collect() // this also works because SomeClass3 is a case class

    // Kryo

    // Using Kryo Encoders with a DataSet is easy:

    implicit val someClassEncoder = Encoders.kryo[SomeClass]
    spark.sparkContext.parallelize(1 to 10).map(x=>new SomeClass(x)).toDF().as[SomeClass].show()

    // but when using Kryo with RDDs, we need to register the class before creating a spark session:

    val conf = new SparkConf().setMaster("local[*]").setAppName("kryo-test")
    conf.registerKryoClasses(Array(classOf[SomeClass]))  // define that SomeClass should be serialized via kryo

    val spark2 = SparkSession.builder().config(conf).getOrCreate()

    import spark2.implicits._
    spark2.sparkContext.parallelize(1 to 10).map(n => new SomeClass(n)).collect() // this works now (although SomeClass is not serializable)

    // to make Kryo the default Serializer

    val conf3 = new SparkConf().setMaster("local[*]").setAppName("kryo-test2")
    conf3.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark3 = SparkSession.builder().config(conf3).getOrCreate()

    import spark3.implicits._
    spark3.sparkContext.parallelize(1 to 10).map(n => new SomeClass(n)).collect() // this also works now


  }

}
