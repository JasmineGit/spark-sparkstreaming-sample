import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext._

/**
 * A sample driver using Spark Streaming and reading from kafka as input stream
 *
 * Using the kafka topic "test"
 *
 * request format for kafka
 *
 * a,100
 * b.10
 * c,1
 * a,1
 * b,1
 * c,1
 *
 */
object DriverWithStream extends App{

  val sparkContext = new SparkContext(new SparkConf().setAppName("TestEngine").setMaster("local[2]"))
  val sparkStreamingContext = new StreamingContext(sparkContext, Milliseconds(2000))


  sparkStreamingContext.checkpoint("/home/cloudera/temp")

  EventBus.initializeStream(sparkStreamingContext)
  val eventStream = EventBus.getMessageStream

  val updatedStream = eventStream.map(_.split(",")).map(arr =>(arr(0),arr(1).toInt)).updateStateByKey(update _)

  updatedStream.foreachRDD(rdd =>{
    println("RDD: "+rdd.id)
    rdd.foreach(println)
  })

  sparkStreamingContext.start()
  sparkStreamingContext.awaitTermination()

  def update(values:Seq[Int], state: Option[Int]):Option[Int]={
    if(values.isEmpty)
      state
    else
      Some(values.sum)
  }
}
