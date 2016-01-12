package org.aspen.consumer;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
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
 * Usage: PDCKafkaConsumer <zkQuorum> <group> <topics> <numThreads>
 *
 * From Gateway node Example
 * spark-submit --class org.aspen.consumer.PDCKafkaConsumer --deploy-mode client --master local[2]
 *   SparkStreamingKafkaConsumer.jar zk.host:2181 pmugrp pmukafkastream 1
 *
 * @TODO run on yarn in distributed mode
 */
public class PDCKafkaConsumer {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: PDCKafkaConsumer <zkQuorum> <group> <topics> <numThreads>");
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

        SparkConf conf = new SparkConf().setAppName("PDCKafkaConsumer");
        conf.set("spark.ui.port","4040");
        JavaStreamingContext ctx = new JavaStreamingContext(conf, new Duration(10000));
        JavaPairReceiverInputDStream<String, String> kfStream = KafkaUtils.createStream(ctx, zkQuorum, kfGrp, topicMap);
        kfStream.saveAsHadoopFiles("/phasor/pmu/pdc", "in", Text.class, Text.class, TextOutputFormat.class);

        ctx.start();
        ctx.awaitTermination();
    }
}
