package secondarysort

import org.apache.spark.sql.SparkSession
import com.tresata.spark.sorted.PairRDDFunctions._
import org.apache.spark.rdd.RDD

object CreateTrajectoriesGroupSort {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("CreateTrajectories using spart-sorted").getOrCreate()

    import spark.implicits._ // required for e.g. toDS
    val rdd: RDD[(Int, AISRecord)] = Utils.getDF(spark).map(r => (r.mmsi, r)).rdd

    val result = rdd
      .groupSort(Ordering.by[AISRecord, Long](_.timestamp.getTime)) // group by mmsi, sort by time
      .mapStreamByKey{toTrajectoryIterator(_)}.values.toDS()

    result.repartition(1)
      .write.mode("overwrite")
      .format("csv").save("/tmp/groupsort.csv")
  }

  def toTrajectoryIterator(itr: Iterator[(AISRecord)]) = {
    new TrajectoryAggregator(itr, new TrajectoryBuilder())
  }
}

/** Creates trajectories from sorted AIS records (implements stop and gap detection) */
class TrajectoryAggregator(sortedAISrecords: Iterator[(AISRecord)], tBuilder: TrajectoryBuilder) extends Iterator[Trajectory] {

  var trajectoryBuilder = tBuilder
  var nextTrajectory = createNext() // implements a lookahead of 1 trajectory

  override def hasNext: Boolean = nextTrajectory != null

  override def next(): Trajectory = {
    val currentResult = nextTrajectory
    nextTrajectory = createNext()
    currentResult
  }

  def createNext(): Trajectory = { // create and return the next trajectory
    while (sortedAISrecords.hasNext) {
      val record: AISRecord = sortedAISrecords.next()

      trajectoryBuilder.addRecord(record)
      // NOTE: trajectory splitting, stop and gap detection logic are added here.
      if(trajectoryBuilder.isTrajectoryFull()) {
        return trajectoryBuilder.buildAndReset()
      }
    }
    // create the final trajectory
    trajectoryBuilder.buildAndReset()
  }
}


