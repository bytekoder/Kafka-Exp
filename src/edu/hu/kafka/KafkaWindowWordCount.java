/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.hu.kafka;

import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <topics>
 * <brokers> is a list of one or more Kafka brokers
 * <topics> is a list of one or more kafka topics to consume from
 * <p>
 * Example:
 * $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port topic1,topic2
 */

public final class KafkaWindowWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
                            "  <brokers> is a list of one or more Kafka brokers\n" +
                            "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];

        // Create context with a 2 seconds batch interval
        JavaSparkContext sparkConf = new JavaSparkContext("local[5]", "JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("zookeeper.connect", "localhost:2181");
        kafkaParams.put("group.id", "spark-app");

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages =
                        KafkaUtils.createDirectStream(jssc, String.class, String.class,
                                        StringDecoder.class, StringDecoder.class, kafkaParams,
                                        topicsSet);

        // Get the lines, split them into words
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {

            @Override
            public String call(Tuple2<String, String> tuple2) {

                return tuple2._2();
            }
        });
        lines.print();

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String x) {

                return Arrays.asList(SPACE.split(x));
            }
        });

        // Reduce function adding two integers, defined separately for clarity
        Function2<Integer, Integer, Integer> reduceFunc =
                        new Function2<Integer, Integer, Integer>() {

                            @Override
                            public Integer call(Integer i1, Integer i2) {

                                return i1 + i2;
                            }
                        };

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs =
                        words.mapToPair(new PairFunction<String, String, Integer>() {

                            @Override
                            public Tuple2<String, Integer> call(String s) {

                                return new Tuple2<String, Integer>(s, 1);
                            }
                        });

      
      /*  */
        // Reduce last 30 seconds of data, every 10 seconds
        JavaPairDStream<String, Integer> windowedWordCounts =
                        pairs.reduceByKeyAndWindow(reduceFunc, Durations.seconds(5),
                                        Durations.seconds(5));

        windowedWordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
