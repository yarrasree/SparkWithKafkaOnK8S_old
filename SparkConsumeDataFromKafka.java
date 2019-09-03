package com.yarra.spark.kafka.kerberos;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;
 
public class SparkConsumeDataFromKafka {
	final static Logger log = Logger.getLogger(SparkConsumeDataFromKafka.class);
    public static JavaSparkContext sparkContext;
 
    public static void main(String[] args) throws InterruptedException {
    	
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "yarraconsumer");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
	    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	   
        //for Kerberos 
        System.setProperty("java.security.auth.login.config","/opt/spark/mnt/jaas/kafka_client_withoutcache_jaas.conf");
    	System.setProperty("java.security.krb5.conf", "/opt/spark/mnt/krb5/krb5.conf");
        
    	kafkaParams.put("sasl.kerberos.service.name", "yarra");
        kafkaParams.put("sasl.mechanism", "GSSAPI");
       
        //for Non-SSL
        //props.put("security.protocol", "SASL_PLAINTEXT");
        
        //for SSL
        kafkaParams.put("security.protocol", "SASL_SSL");
        kafkaParams.put("ssl.truststore.location", "/opt/spark/mnt/truststore/kafka.client.truststore.jks");
        kafkaParams.put("ssl.truststore.password", "clientpass");
        Collection<String> topics = Arrays.asList("yarra");
 
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Spark Consume data from Kafka component");
      
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
 
        sparkContext = streamingContext.sparkContext();
        Random random = new Random();
        
        streamingContext.checkpoint("hdfs://namenodeHA/tmp/kafkaCheckPoint"+random.nextInt(100));
 
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));
 
        JavaPairDStream<String, String> results = messages.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
 
        JavaDStream<String> lines = results.map(tuple2 -> tuple2._2());
 
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split("\\s+"))
                .iterator());
 
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);
 
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> cumulativeWordCounts = wordCounts.mapWithState(StateSpec.function((word, one, state) -> {
            int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
            Tuple2<String, Integer> output = new Tuple2<>(word, sum);
            state.update(sum);
            return output;
        }));
 
        cumulativeWordCounts.foreachRDD(javaRdd -> {
            List<Tuple2<String, Integer>> wordCountList = javaRdd.collect();
            for (Tuple2<String, Integer> tuple : wordCountList) {
                List<Word> wordList = Arrays.asList(new Word(tuple._1, tuple._2));
                for (Word word: wordList
                     ) {
                    log.info("Yarra ++++++++++++++++++++++++++++++ >>>>>>>>>>>>>>>>>  "+word.getWord()+" "+word.getCount());
                }
 
            }
        });
 
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
