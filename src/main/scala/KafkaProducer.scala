import java.util.Properties

import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

/**
 * Created by cloudera on 2/9/15.
 */
object  KafkaProducer {
  val topic: String = "test"

  val effectiveConfig = {
    val c = new Properties
    c.put("metadata.broker.list", "localhost:9092")
    c.put("topic", "test")
    c
  }
  val producer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(effectiveConfig))


  // The configuration field of the wrapped producer is immutable (including its nested fields), so it's safe to expose
  // it directly.


  def kafkaMsg(message: Array[Byte]): KeyedMessage[Array[Byte], Array[Byte]] = {
    new KeyedMessage(topic, message)
  }

  def send(message: String): Unit = send(message.getBytes("UTF8"))

  type Key = Array[Byte]
  type Val = Array[Byte]

  def send(message: Array[Byte]): Unit = {
    try {
      val result = kafkaMsg(message)
      producer.send(result)
    } catch {
      case e: Exception =>
        e.printStackTrace
        //Todo Handle this
        System.exit(1)
    }
  }
}
