import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by cloudera on 2/9/15.
 */

object EventBus{

  //Kafka Consumer related params
  var recieverInputStream : ReceiverInputDStream[(String, String)] = _
  private var _zookeeperConnection:String = _
  private var _groupId:String = _
  private var _topic: String = _
  private var _storageLevel:StorageLevel = _

  // Constants
  private val topicNameString = "topic.name"
  private val zookeeperConnectStr ="zookeeperconnect"
  private val groupStr ="group"
  private val rootStr ="kafka"

  private val brokerListKeyStr ="metadata.broker.list"
  private val maxProducers = 10
  private val brokerList = "brokerlist"
  private val producerType = "producer.type"


  def initializeStream( ssc: StreamingContext) {
    _topic = "test"
    _zookeeperConnection = "localhost:2181"
    _groupId = "groupA"

    // TODO Suresh - We have 8 core CPU, but the partition/threads is only 1 ????
    val topics = Map(
      _topic -> 1
    )
    recieverInputStream = KafkaUtils.createStream(ssc, _zookeeperConnection, _groupId, topics, StorageLevel.MEMORY_AND_DISK_SER_2)
  }

  def getMessageStream: DStream[String]={
    val messageDStream = recieverInputStream.map{
      case(topic,data) => data
    }
    messageDStream
  }
}
