package secondarysort

import java.sql.Timestamp

import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.rdd.RDD

case class AISKey(mmsi: Int, timestamp: Timestamp)

object AISKey {
  implicit def orderingByIdAndTimestamp[A <: AISKey] : Ordering[A] = {
    Ordering.by(fk => (fk.mmsi, fk.timestamp.getTime))
  }
}

class ShipPartitioner(val partitioner: Partitioner) extends Partitioner {
  override def numPartitions: Int = partitioner.numPartitions

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[AISKey]
    partitioner.getPartition(k.mmsi)
  }
}

object CreateTrajectoriesCoreOnly {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .master("local")
      .appName("CreateTrajectories using SparkCore Only").getOrCreate()

    import spark.implicits._ // required for toDS
    val rdd: RDD[(AISKey, AISRecord)] = Utils.getDF(spark)
      .map(r => (AISKey(r.mmsi, r.timestamp), r)).rdd

    val result = rdd
      .repartitionAndSortWithinPartitions(new ShipPartitioner(defaultPartitioner(rdd)))
      .mapPartitions(toTrajectoryIterator(_)).toDS()

    result
      .repartition(1)
      .write.mode("overwrite")
      .format("csv").save("/tmp/coreonly.csv")
  }

  def toTrajectoryIterator(itr: Iterator[(AISKey, AISRecord)]) = {
    new NewTrajectoryAggregator(itr, new TrajectoryBuilder())
  }
}

/** Creates trajectories from sorted AIS records (implements stop and gap detection) */
class NewTrajectoryAggregator(sortedAISrecords: Iterator[(AISKey, AISRecord)], tBuilder: TrajectoryBuilder) extends Iterator[Trajectory] {
  var currentMMSI: Option[Int] = Option.empty
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
      val record: AISRecord = sortedAISrecords.next()._2

      if (currentMMSI.isEmpty)
        currentMMSI = Some(record.mmsi)

      if (record.mmsi == currentMMSI.get && !trajectoryBuilder.isTrajectoryFull()) {
        trajectoryBuilder.addRecord(record)
      } else { // finish the current trajectory and start a new one
        val result: Trajectory = trajectoryBuilder.buildAndReset()
        trajectoryBuilder.addRecord(record)
        currentMMSI = Some(record.mmsi)

        return result
      }
    }
    // all records have been processed. Create a final trajectory, if any
    trajectoryBuilder.buildAndReset()
  }
}

