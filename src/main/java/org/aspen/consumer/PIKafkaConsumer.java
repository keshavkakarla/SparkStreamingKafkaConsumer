package org.aspen.consumer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Mike Mengarelli, Cloudera on 11/13/15
 *
 * Remedial Kafka Consumer using Spark Streaming
 *
 * Usage: PDCKafkaConsumer <zkQuorum> <group> <topics> <numThreads>
 *
 * From Gateway node Example:
 * spark-submit --class org.aspen.consumer.PIKafkaConsumer --deploy-mode client --master local[5]
 *   SparkStreamingKafkaConsumer.jar zk_host:2181 pigrp pikafkastream 1
 *
 * @TODO run on yarn in distributed mode
 */
public class PIKafkaConsumer {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: PDCKafkaConsumer <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

        String zkQuorum = args[0];
        String kfGrp = args[1];
        String[] topics = args[2].split(",");
        int numThreads = Integer.valueOf(args[3]);

        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        SparkConf conf = new SparkConf().setAppName("PIConsumer");
        JavaStreamingContext ctx = new JavaStreamingContext(conf, new Duration(2000));
        JavaPairReceiverInputDStream<String, String> kfStream = KafkaUtils.createStream(ctx, zkQuorum, kfGrp, topicMap);
        kfStream.saveAsHadoopFiles("PI", "in", Text.class, Text.class, TextOutputFormat.class);

        ctx.start();
        ctx.awaitTermination();
    }
}
