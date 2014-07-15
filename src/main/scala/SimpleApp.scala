
import org.apache.hadoop.io.{Text, BytesWritable}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD



object SimpleApp {
  abstract class AggregateCheques extends Serializable{
    def aggregate(cheques: Iterable[Cheque]): Float
    def name(): String
  }

  class CountCheques extends AggregateCheques with Serializable {
    override def aggregate(cheques: Iterable[Cheque]): Float = cheques.size

    override def name(): String = "cheques_count"
  }



  def parseTransaction(sourceFile: RDD[String]) = {
    val transactions =
      sourceFile.
        filter(line => Character.isDigit(line.charAt(0))).
        map(Transaction(_, sep = ","))

    transactions
  }

  def parseGoodsGroups(sourceFile: RDD[String]) = {
    def parseGoodGroupLine(line: String, sep: String) = {
      val parts = line.split(sep)
      assert(parts.length == 2)
      (parts(0), parts(1))
    }
    val goodsGroups =
      sourceFile.
        map(parseGoodGroupLine(_, sep = "\t"))

    goodsGroups
  }

  def gatherCheques(transactions: RDD[Transaction]) = {
    val cheques =
      transactions.
        map(t => (t.chequeID, t)).
        groupByKey().
        map(t => Cheque(t._2))

    cheques
  }

  def replaceGoodsByGroupsInCheques(cheques: RDD[Cheque], goodGroupMap: Map[String, String]) = {
    def replaceGoodsByGroupsInCheque(c: Cheque): Cheque = {
      val groupIDList = c.goodIDList.flatMap(goodGroupMap.get(_))
      new Cheque(c.chequeID, c.userID, groupIDList, c.startTimestamp, c.endTimestamp)
    }
    cheques.map(replaceGoodsByGroupsInCheque)
  }


  def main(args: Array[String]) {
    val transactionsFile = "data/transactions.csv" // Should be some file on your system
    val goodsGroupsFile = "data/goods_groups.seq" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val transactions = parseTransaction(sc.textFile(transactionsFile, 2))
    val cheques = gatherCheques(transactions)

    val goodsGroupsSourceRDD = sc.sequenceFile(goodsGroupsFile, classOf[BytesWritable], classOf[Text])
    val goodsGroups = parseGoodsGroups(goodsGroupsSourceRDD.map(_._2.toString))
    val goodsGroupsList = goodsGroups.collect()
    val goodsGroupsMap = goodsGroupsList.groupBy(_._1).mapValues(_(0)._2).map(identity)
    val groupCheques = replaceGoodsByGroupsInCheques(cheques, goodsGroupsMap).cache()

  }


}