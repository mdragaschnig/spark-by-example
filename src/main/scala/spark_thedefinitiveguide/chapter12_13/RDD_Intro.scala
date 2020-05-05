package spark_thedefinitiveguide.chapter12_13

import java.lang

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object RDD_Intro {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Experiments").getOrCreate()
    import spark.implicits._

    // access the underlying RDD of a DataFrame / DataSet
    val ds: Dataset[lang.Long] = spark.range(500)
    val rdd: RDD[lang.Long] = ds.rdd

    // covert rdd to DataFrame or DataSet
    val df: DataFrame = rdd.toDF()
    val ds2: Dataset[lang.Long] = rdd.toDS()

    // create RDD from a local collection
    val words: RDD[String] = spark.sparkContext.parallelize(Seq("here", "are", "some", "words", "for", "you"), 2)
    words.setName("my_words")  // name shows up in sparkUi

    // manipulations
    words.distinct().count()
    words.filter(word => word.startsWith("f")).collect()
    words.map(word => "!!" + word + "!!").collect()
    words.sortBy(word => word).collect()

    // reduction
    words.reduce((w1, w2) => if(w1.length<w2.length) w1 else w2)  // find shortest word

    // checkpointing
    spark.sparkContext.setCheckpointDir("/tmp/spark/")
    words.checkpoint()

    // pipe

    // process each partition using an external shell command (stdin=partition content, stdout will be transformed to a new RDD partition)
    words.pipe("wc -l").collect()   // a new wc process is created once for each partition
    words.repartition(10).pipe("wc -l").collect()  // now we get 10 calls to wc  (some of them on empty partitions)

    //
    // mapPartitions

    // maps an entire partition (input: Iterator[Record]   output: Iterator[OtherRecord])
    words.mapPartitions(itr =>   Seq(itr.length).iterator).collect()   // map each partition to its record count

    words.mapPartitionsWithIndex((idx, partItr) =>  partItr.map(w => s"$idx -> $w")).collect()

    // forEachPartition (just iterates the partitions, returns no result)
    words.foreachPartition( partItr => partItr.foreach(print(_)))  // not a very good example (on a cluster the print would happen on a worker node, not the driver!)

    // glom  (transform each partition into an array of records)
    val parts: RDD[Array[String]]  = words.glom()





  }



}
