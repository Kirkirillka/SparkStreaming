import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object SimpleStreamingContext {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("spark-streaming")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    ssc.start()
    ssc.awaitTermination()


  }
}
