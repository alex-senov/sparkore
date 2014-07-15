/**
 * Created by senov on 7/15/14.
 */
class Transaction (val chequeID: String,
                   val userID: String,
                   val goodID: String,
                   val timestamp: Long) extends Serializable {}

object Transaction {
  def apply(line: String, sep: String) = {
    val parts = line.split(sep)
    new Transaction(
      parts(0),
      parts(1),
      parts(2),
      parts(3).toLong)
  }
}