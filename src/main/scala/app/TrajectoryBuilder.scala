package app

import scala.collection.mutable

class TrajectoryBuilder() {
  val records = mutable.ListBuffer[AISRecord]()
  val trajectorySize = 500

  def isEmpty = records.isEmpty

  def isTrajectoryInProgress = !isEmpty

  def trajectoryDurationMs = if (records.isEmpty) 0L else (records.last.timestamp.getTime - records(0).timestamp.getTime)

  def isTrajectoryFull() = records.size == trajectorySize

  def addRecord(r: AISRecord)  {
    if(!isEmpty) {  // sanity checks
      if(timeDiffTo(r) < 0)
        throw new IllegalArgumentException(s"${r.mmsi} has unsorted data: got ${r} but latest seen record is ${records.last}")
      if(records.last.mmsi != r.mmsi)
        throw new IllegalArgumentException("MMSI changed.")
    }
    records += r
  }

  def buildAndReset(): Trajectory = {
    var result: Trajectory = null
    if(records.nonEmpty) {
      val firstTimestamp = records.head.timestamp
      val lastTimestamp = records.last.timestamp
      result = Trajectory(records.head.mmsi, firstTimestamp, lastTimestamp, records.size )
    }
    reset()
    result
  }

  def reset() {
    records.clear()
  }

  /** @return positive integer if time of provided record is after the last trajectory record  */
  def timeDiffTo(someLaterRecord: AISRecord) = someLaterRecord.timestamp.getTime - records.last.timestamp.getTime

  def createTimes(): Array[Long] = records.map(r => r.timestamp.getTime).toArray
}

