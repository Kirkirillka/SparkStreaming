import com.google.gson.Gson
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils



object utils {

  // save tweetes in file
  def saveTweets(stream: ReceiverInputDStream[Any] ): Unit ={

    val tweets = stream.map(new Gson().toJson(_))


    val numTweetsCollect = 10000L
    var numTweetsCollected = 0L

    tweets.foreachRDD((rdd, time) =>{
      val count = rdd.count()
      if (count >0){
        val outputRDD = rdd.coalesce(1)
        outputRDD.saveAsTextFile("data/tweets.json")
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsCollected){
          System.exit(0)
        }
      }
    })
  }
}
