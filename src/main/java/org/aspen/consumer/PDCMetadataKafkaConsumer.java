package org.aspen.consumer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Mike Mengarelli, Cloudera on 11/13/15
 * <p/>
 * Remedial Kafka Consumer using Spark Streaming
 * <p/>
 * Usage: PDCMetadataKafkaConsumer <zkQuorum> <group> <topics> <numThreads>
 *
 * From Gateway node Example
 * spark-submit --class org.aspen.consumer.PDCMetadataKafkaConsumer --deploy-mode client --master local[5]
 *   SparkStreamingKafkaConsumer.jar ip-10-6-8-56:2181 metagrp pmukafkastream-metadata 1
 *
 * @TODO run on yarn in distributed mode
 */
public class PDCMetadataKafkaConsumer {
    private static final Logger _LOG = Logger.getLogger(PDCMetadataKafkaConsumer.class);

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: PDCMetadataKafkaConsumer <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

        String zkQuorum = args[0];
        String kfGrp = args[1];
        String[] topics = args[2].split(",");
        int numThreads = Integer.valueOf(args[3]);

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        SparkConf conf = new SparkConf().setAppName("PDCMetadataKafkaConsumer");
        JavaStreamingContext ctx = new JavaStreamingContext(conf, new Duration(2000));
        JavaPairReceiverInputDStream<String, String> kfStream = KafkaUtils.createStream(ctx, zkQuorum, kfGrp, topicMap);

        //Filter out un-needed messages
        //For the first message, the key is an 8-byte integer that represents the current time
        //and the value is another 8-byte integer that represents the size of the XML message that follows.
        JavaPairDStream<String, String> fStream = kfStream.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                return tuple._2().length() > 16;
            }
        });

        fStream.saveAsHadoopFiles("PDC-MD", "in", Text.class, Text.class, TextOutputFormat.class);

        ctx.start();
        ctx.awaitTermination();
    }
}
