import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by cloudera on 2/3/15.
 */
object Driver extends App{

  val sc = new SparkContext(new SparkConf().setAppName("TestEngine").setMaster("local[2]"))

  val seqInts = (for(i <- 0 to 2000) yield i )

  val rddInts = sc.parallelize(seqInts)

  rddInts.foreach(i => {

    println(i)

  })

}
