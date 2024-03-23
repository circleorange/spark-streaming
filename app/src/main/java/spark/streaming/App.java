package spark.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

@SuppressWarnings("deprecation")
public class App {
    public static void main(String[] args) 
        throws InterruptedException {

        // initalize Spark configuration with two local working threads
        SparkConf conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("TwotterStreamProcessor");

        // initalize Spark Streaming Context with two second batch duration
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));

        // connect Twotter server using socket stream
        JavaReceiverInputDStream<String> tweets = jssc.socketTextStream("localhost", 9999);

        // print sample
        tweets.print();

        // extract hashtags and usernames
        JavaDStream<String> tagsAndUsernames = tweets
            .flatMap(tweet -> Arrays.asList(tweet.split(" ")).iterator())
            .filter(word -> word.startsWith("#") || word.startsWith("@"));

        // count occurrances in each window
        JavaPairDStream<String, Integer> tagsAndUsernameCount = tagsAndUsernames
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKeyAndWindow(
                (Integer i, Integer j) -> i + j,
                Durations.seconds(6), // window length
                Durations.seconds(2)); // sliding interval
        
        // print count of occurrances
        tagsAndUsernameCount.print();

        // start computation
        jssc.start();

        // close the streaming context after computation is finished
        jssc.awaitTermination();
    }
}
