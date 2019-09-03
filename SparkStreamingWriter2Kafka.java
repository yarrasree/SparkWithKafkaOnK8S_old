package com.yarra.spark.kafka.kerberos;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamingWriter2Kafka {
	final static Logger log = Logger.getLogger(SparkStreamingWriter2Kafka.class);
	private static final FlatMapFunction<String, String> WORDS_EXTRACTOR = new FlatMapFunction<String, String>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Iterator<String> call(String s) throws Exception {
			return Arrays.asList(s.split(" ")).iterator();
		}
	};

	public static void main(String[] args) throws Exception {
		try {
		System.setProperty("java.security.auth.login.config","/opt/spark/mnt/jaas/kafka_client_withoutcache_jaas.conf");
		System.setProperty("java.security.krb5.conf", "/opt/spark/mnt/krb5/krb5.conf");
		System.setProperty("sun.security.krb5.debug", "false");
	
		
		SparkConf conf = new SparkConf();
		conf.setAppName("Spark streaming Writer 2 Kafka");
		//conf.setMaster(args[1]);

		JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(100));
		Random random = new Random();
		ssc.checkpoint("hdfs://namenodeHA/tmp/kafkaCheckPoint"+random.nextInt(100));
		JavaDStream<String> lines = ssc.textFileStream("hdfs://namenodeHA/tmp/yarra");
		JavaDStream<String> words = lines.flatMap(WORDS_EXTRACTOR);
		words.print();
		
		words.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			@Override
			public void call(JavaRDD<String> rdd) throws Exception {
				Future<RecordMetadata> fu = null;
				
				Producer<String, String> producer = YarraKafkaProducer.getProducer(args[0]);
				for (String line : rdd.collect()) {
					log.info("before write to kafka >>>>>>>>>>>>>>>>>>>>>>> "+line);
					if (line != null && line.trim().length() > 0) {
						try {
						fu = producer.send(new ProducerRecord<String, String>("yarra", line, line));
						 if(fu.get() != null)
							 log.info("*************************** offset:"+fu.get().offset() +", topic:"+fu.get().topic()+", partition:"+fu.get().partition());
						 
						} catch(Exception ex) {
							ex.printStackTrace();
							log.error("Exception @ "+ex.getMessage());
						}
					}
					
				}

			}
		});

		ssc.start();
		ssc.awaitTermination();
		}catch(Exception ex) {
			ex.printStackTrace();
			log.error("Exception at Driver program : "+ex.getMessage());
		}
	}
}
