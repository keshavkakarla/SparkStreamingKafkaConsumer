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
 * <p/>
 * From Gateway node Example
 * spark-submit --class org.aspen.consumer.PDCMetadataKafkaConsumer --deploy-mode client --master local[2]
 * SparkStreamingKafkaConsumer.jar ip-10-6-8-56:2181 metagrp pmumetadatstream 1
 *
 * @TODO run on yarn in distributed mode
 */
public class PDCMetadataKafkaConsumer {
    private static final Logger _LOG = Logger.getLogger(PDCMetadataKafkaConsumer.class);

    public static void main(String[] args) {
        if (args.length < 4) {
            logError("Usage: PDCMetadataKafkaConsumer <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

        String zkQuorum = args[0];
        String kfGrp = args[1];
        String[] topics = args[2].split(",");
        int numThreads = Integer.valueOf(args[3]);

        if (topics.length == 0) {
            logError("Usage: Please supply at least one kafka topic.");
            System.exit(1);
        }

        _LOG.debug("zkQuorum: " + zkQuorum);
        _LOG.debug("group: " + kfGrp);
        _LOG.debug("topic[0]: " + topics[0]);
        _LOG.debug("threads: " + numThreads);

        if (zkQuorum.indexOf(":") < 2) {
            logError("Usage: Please supply a proper zkQuorum. Ex: host.name:2181");
            System.exit(1);
        }

        if (kfGrp.length() < 1) {
            logError("Usage: Please supply a proper kafka group name");
            System.exit(1);
        }

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        SparkConf conf = new SparkConf().setAppName("PDCMetadataKafkaConsumer");
        conf.set("spark.ui.port", "4050");
        _LOG.debug("Set spark to run on port 4050");

        JavaStreamingContext ctx = new JavaStreamingContext(conf, new Duration(1000));
        JavaPairReceiverInputDStream<String, String> kfStream = KafkaUtils.createStream(ctx, zkQuorum, kfGrp, topicMap);

        //Filter out un-needed messages
        //For the first message, the key is an 8-byte integer that represents the current time
        //and the value is another 8-byte integer that represents the size of the XML message that follows.
        JavaPairDStream<String, String> fStream = kfStream.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                boolean result = tuple._2().length() > 16;
                if (_LOG.isDebugEnabled()) {
                    _LOG.debug(String.format("Tuple: %s, Saving to HDFS: %s", tuple._2(), result));
                }
                return result;
            }
        });

        fStream.saveAsHadoopFiles("/phasor/pmumetadata/pdc-md", "in", Text.class, Text.class, TextOutputFormat.class);

        ctx.start();
        ctx.awaitTermination();
    }

    private static final void logError(String msg) {
        System.err.println(msg);
        _LOG.error(msg);
    }
}
