import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import settings._
//import utils.saveTweets

object SparkTwitterStreaming {
  def main(args: Array[String]): Unit = {

    // Suppress unessesary log output
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf()
    conf.setAppName("spark-streaming")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))


    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Create Twitter Stream
    val stream = TwitterUtils.createStream(ssc, None)
    val tweets = stream.map(t => t.getText)

    //B.2.1 Filter out by messages starting with #
    val  reg= """#(.*)\s""".r
    //RDD[Array(String)] - if we have simple map
    //RDD[String] - if we use flatMap
    val hashtags = tweets.flatMap(x=>x.split(" ")).filter(x=>x.startsWith("#"))

    //Print streamed hastags
    //hashtags.print()

    //B.2.2 Find the 10 top hastags based on their counts
    val counts = hashtags.countByValue()

    //Print out hashtags in descending order
    counts.print()
    //counts.foreachRDD(x=>println(s"$x(1)"))


    //C
    val new_tweets = tweets.map(new Gson().toJson(_))

    val numTweetsCollect = 10L
    var numTweetsCollected = 0L

    System.setProperty("hadoop.home.dir", "data")

    new_tweets.foreachRDD((rdd, time) =>{
      val count = rdd.count()
      if (count >0){
        val outputRDD = rdd.coalesce(1)
        outputRDD.saveAsTextFile("data")
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsCollect){
          System.exit(0)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}