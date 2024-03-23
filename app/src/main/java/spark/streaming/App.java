package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

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

        // start computation
        jssc.start();

        // close the streaming context after computation is finished
        jssc.awaitTermination();
    }
}
