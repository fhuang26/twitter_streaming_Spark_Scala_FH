
# Twitter Tweets Sentiment Analysis by Spark Streaming with Scala
     to leverage Spark Streaming component to consume Twitter data and perform sentiment analysis on it.
       
 *   For each tweet, break the message into tokens, then remove punctuation marks and stop words.
 *   Simple sentiment analysis: to determine the score of whether a tweet has a positive, negative or neutral sentiment.
 *   A list of positive and negative words are provided.
 *   To calculate number of positive, negative and neutral words in tweets and print out them using window length of 10 and 30 seconds (every 10 sec to report; 30 sec to reset).
 *   Display the score and sentiment of streaming tweets as positive, negative or neutral based on 10-sec window.
 *   score = ((# positive words in tweets) - (# of negative words in tweets))/(total # words)
 *   (1)  score >= 1%  : positive
     (2)  score <= -1% : negative
     (3)  |score| < 1% : neutral
 
