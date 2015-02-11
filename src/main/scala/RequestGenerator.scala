/**
 * Created by cloudera on 2/9/15.
 */
object RequestGenerator extends App{

  val keySet = Array[String]("aaa","bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii")

  val data = for{
    key <- keySet
    i <- 1 to 1000
  }yield Array(key, 1)

  data.map(keyValuePair => {
    KafkaProducer.send(keyValuePair.mkString(","))
  })

}
