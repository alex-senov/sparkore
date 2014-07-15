/**
 * Created by senov on 7/15/14.
 */
class Cheque (val chequeID: String,
              val userID: String,
              val goodIDList: List[String],
              val startTimestamp: Long,
              val endTimestamp: Long) extends Serializable {}


object Cheque {
  def apply (tIt: Iterable[Transaction]) = {
    val tList = tIt.toList
    val goodIDList = tList.map(_.goodID)
    val maxTimestamp = tList.map(_.timestamp).reduce((x: Long, y: Long) => math.max(x, y))
    val minTimestamp = tList.map(_.timestamp).reduce((x: Long, y: Long) => math.min(x, y))
    new Cheque(tList(0).chequeID,
      tList(0).userID,
      goodIDList,
      maxTimestamp,
      minTimestamp)
  }
}
