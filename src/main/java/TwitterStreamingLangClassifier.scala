import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import settings.{accessToken, accessTokenSecret, apiKey, apiSecret}

object TwitterStreamingLangClassifier {


  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "data")
    val dir_path = "data/"

    val conf = new SparkConf()
    conf.setAppName("twitter_lang_class")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))


    //Setting Twitter credentials
    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val tweets = TwitterUtils.createStream(ssc, None)

    //Load text
    val text = tweets.map(_.getText)
    text.print()

    //Get the features vector
    val features = text.map(featurize)

    val numClusters = 10
    val numIterations = 40

    // Train KMenas model and save it to file
    val model = KMeans.train(features, numClusters, numIterations)
    model.save(sc, "data/model")

  }


}
