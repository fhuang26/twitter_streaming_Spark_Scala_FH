import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils

import java.util.Date
import org.apache.log4j.{Level, Logger}

/**
 *
 *  Twitter Tweets Sentiment Analysis	by Spark Streaming with Scala             Felix Huang
 *    to leverage Spark Streaming component to consume Twitter data and perform sentiment
 *    analysis on it.
 *      
 *   (1) For each tweet, break the message into tokens, then remove punctuation marks and
 *       stop words.
 *   (2) Simple sentiment analysis: to determine the score of whether a tweet has a positive,
 *       negative or neutral sentiment.
 *   (3) A list of positive and negative words are provided.
 *   (4) To calculate number of positive, negative and neutral words in tweets and print out
 *       them using window length of 10 and 30 seconds (every 10 sec to report; 30 sec to reset).
 *   (5) Display the score and sentiment of streaming tweets as positive, negative or neutral
 *       based on 10-sec window.
 *   (6) score = ((# positive words in tweets) - (# of negative words in tweets))/(total # words)
 *         score >= 1%  : positive
 *         score <= -1% : negative
 *         |score| < 1% : neutral
 *  
 *
 *  To run this app
 *   ./sbt/sbt assembly
 *   $SPARK_HOME/bin/spark-submit --class "TwitterStreamingApp" --master local[*] ./target/scala-2.10/twitter-streaming-assembly-1.0.jar
 *      <consumer key> <consumer secret> <access token> <access token secret>
 */
object TwitterStreamingApp {
  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: TwitterStreamingApp <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    //val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    // to run this application inside an IDE, comment out previous line and uncomment line below
    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]")

       
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val stream = TwitterUtils.createStream(ssc, None, filters);
    
    //val stream = TwitterUtils.createStream(ssc, None, filters)

    val stopWordsRDD = ssc.sparkContext.textFile("src/main/resources/stop-words.txt")
    val posWordsRDD = ssc.sparkContext.textFile("src/main/resources/pos-words.txt")
    val negWordsRDD = ssc.sparkContext.textFile("src/main/resources/neg-words.txt")

    val positiveWords = posWordsRDD.collect().toSet
    val negativeWords = negWordsRDD.collect().toSet
    val stopWords = stopWordsRDD.collect().toSet

    val englishTweets = stream.window(Seconds(30), Seconds(10)).filter(status => status.getUser().getLang() == "en")
    
    // to verify input tweets
    val tweets = englishTweets.map(status => status.getText())
    // tweets.print()
    
    val t2 = tweets.map(l => l.toLowerCase()).flatMap(l => l.split(" "))
    // t2.print()
    
    // remove punctuation and digits; keep only letters
    val t3 = t2.map(a => a.filter(_.isLetter))
    // t3.print()
    
    // filter out stopWords
    val t9 = t3.filter(w => !stopWords.contains(w))
    // t9.print()
    
    def typeWords(w:String) : (String,Int) = {
      if (positiveWords.contains(w)) return ("positive",1)
      if (negativeWords.contains(w)) return ("negative",1)
      return ("neutral",1)
    }
    // count positive, negative, and neutral words
    val t7 = t9.map(w => typeWords(w)).reduceByKey(_+_)
    // t7.print()
    
    // to calculate total number of words in tweets in the current window
    val t6 = t7.map(p => p._2).reduce(_+_).map(v => ("total",v))
    // t6.print()
    
    val t8 = t7.union(t6)
    t8.print()
    
    val nPosWords = t8.filter(_._1 == "positive").map(p => p._2).map(v => ("rec", v))
    val nNegWords = t8.filter(_._1 == "negative").map(p => p._2).map(v => ("rec", v))
    val total = t8.filter(_._1 == "total").map(p => p._2).map(v => ("rec", v))
    val oneRec = nPosWords.leftOuterJoin(nNegWords).leftOuterJoin(total).map(a => (a._2._1._1,
        a._2._1._2.get.toInt, a._2._2.get.toDouble))
    // oneRec.print()
    
    def getScore(t:(Int,Int,Double)) : (String,Double,String,String) = {
      if (t._3 == 0.0) return ("score",0.0,"sentiment","neutral")
      val score = (t._1 - t._2) / t._3
      if (score >= 0.01) return ("score",score,"sentiment","positive")
      else if (score <= -0.01) return ("score",score,"sentiment","negative")
      else return ("score",score,"sentiment","neutral")
    }
    val sentiment = oneRec.map(t => getScore(t))
    sentiment.print()
    // sentiment.saveAsTextFiles("src/main/resources/output")
    
    ssc.start()
    ssc.awaitTermination()
  }

}