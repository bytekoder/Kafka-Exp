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
 * Created by bhavanishekhawat
 *
 * */
public class JavaDirectKafkaWordCount {

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

        JavaSparkContext sparkConf = new JavaSparkContext("local[5]", "JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("zookeeper.connect", "localhost:21811");
        kafkaParams.put("group.id", "spark-app");
        System.out.println("Kafka parameters: " + kafkaParams);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages =
                        KafkaUtils.createDirectStream(jssc, String.class, String.class,
                                        StringDecoder.class, StringDecoder.class, kafkaParams,
                                        topicsSet);

        // Get the lines, split them into words
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                System.out.println("*************MY OUTPUT: processing lines: tuple2._1() = " +
                                tuple2._1() + "; tuple2._2()=" + tuple2._2());
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
        //words.print();

        // Reduce function adding two integers, defined separately for clarity
        Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
            @Override public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        };

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                        new PairFunction<String, String, Integer>() {
                            @Override public Tuple2<String, Integer> call(String s) {
                                return new Tuple2<String, Integer>(s, 1);
                            }
                        });
    /*
    JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        @Override public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });

      /*  */
        // Reduce last 30 seconds of data, every 10 seconds
        JavaPairDStream<String, Integer> windowedWordCounts = pairs.reduceByKeyAndWindow(
                        reduceFunc, Durations.seconds(30), Durations.seconds(10));

        windowedWordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
