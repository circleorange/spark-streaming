package spark.streaming;

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;

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

        // 2/occurrances

        // extract hashtags and usernames
        JavaDStream<String> tagsAndUsernames = tweets
            .flatMap(tweet -> Arrays.asList(tweet.split(" ")).iterator())
            .filter(word -> word.startsWith("#") || word.startsWith("@"));

        // count occurrances in each window
        JavaPairDStream<String, Integer> tagsAndUsernameCount = tagsAndUsernames
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKeyAndWindow(
                (Integer i, Integer j) -> i + j, // reduce function
                Durations.seconds(6), // window duration
                Durations.seconds(2) ); // slide duration
        
        // print count of occurrances
        tagsAndUsernameCount.print();

        // 3/ highest frequency hashtag

        JavaDStream<String> tags = tweets
            // split the incoming tweet data with space delimiter
            .flatMap( t -> Arrays.asList( t.split(" ")).iterator()) 
            // extract strings starting with hashtag
            .filter( s -> s.startsWith("#")); 
        
        // count occurrances of each tag in the data stream
        JavaPairDStream<String, Integer> tagCount = tags
            // map each tag occurrance to 1, i.e. single occurrance
            .mapToPair( t -> new Tuple2<>(t, 1)) 
            // aggregate occurrances by tag (key) over 6 second window that slides every 2 seconds
            .reduceByKeyAndWindow( 
                Integer::sum, Durations.seconds(6), Durations.seconds(2));

        // identify highest frequency tag in each sliding window
        JavaPairDStream<String, Integer> tagHighestFrequency = tagCount
            .transformToPair( rdd -> { return jssc
                // retrieve Spark context JavaStreamingContext
                .sparkContext()
                // convert Java list returned from `take()` to RDD
                .parallelizePairs( rdd
                    // swap (hashtag, count) to (count, hashtag) - needed for ordering data by key
                    .mapToPair(Tuple2::swap)
                    // order data by key in descending order
                    .sortByKey(false)
                    // swap back to original structure; (hashtag, count)
                    .mapToPair(Tuple2::swap)
                    // take the first (i.e. highest) result
                    .take(1) );
            });

        tagHighestFrequency.print();


        // start computation
        jssc.start();

        // close the streaming context after computation is finished
        jssc.awaitTermination();
        jssc.close();
    }
}
