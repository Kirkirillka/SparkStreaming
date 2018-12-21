import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object ClassifyLanguage {

  //Featurize Function
  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }

  def main(args: Array[String]): Unit = {

    val modelInput = args(0)

    System.setProperty("hadoop.home.dir", "data");

    val conf = new SparkConf()
    conf.setAppName("language-classifier")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))

    // Configure your Twitter credentials
    val apiKey = "KNVTwiYajvHab5NBqBWSWepde"
    val apiSecret = "62eWYe5bBZlf09mXzpdgtOAzSBm7FxlkoZCgCYmRblpTOSI4wu"
    val accessToken = "863078539347861504-eWeovitnbZ2g8eNinenoMTz1PTfMbOY"
    val accessTokenSecret = "UP1tsTaFuVXTmDJ3nIxGYo0mhqt3ybOuClEQ1V4oTWvbT"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Create Twitter Stream
    println("Initializing Twitter stream...")
    val tweets = TwitterUtils
      .createStream(ssc, None)

    val texts = tweets.map(_.getText)

    println("Initializing the KMeans model...")
    val model = KMeansModel.load(sc, modelInput)
    val langNumber = 3

    val filtered = texts.filter(t => model.predict(featurize(t)) == langNumber)
    filtered.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
