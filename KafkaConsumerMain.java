package com.yarra.spark.kafka.kerberos;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;


public class KafkaConsumerMain {

	public static void main(String[] args) {
		System.setProperty("java.security.auth.login.config","/opt/spark/mnt/jaas/kafka_client_withoutcache_jaas.conf");
		System.setProperty("java.security.krb5.conf", "/opt/spark/mnt/krb5/krb5.conf");
	
		String bootstrapserver = args[0];
		Properties props = new Properties();
		
	     props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
	     props.put(ConsumerConfig.GROUP_ID_CONFIG, "yarraconsumer");
	     props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
	     props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	    
         props.put("sasl.kerberos.service.name", "yarra");
         props.put("sasl.mechanism", "GSSAPI");

         //non SSL
         //props.put("security.protocol", "SASL_PLAINTEXT");
         //for SSL
         props.put("security.protocol", "SASL_SSL");
         props.put("ssl.truststore.location", "/opt/spark/conf/kafka.client.truststore.jks");
         props.put("ssl.truststore.password", "clientpass");
         org.apache.kafka.clients.consumer.KafkaConsumer<String,String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String,String>(props);
	     consumer.subscribe(Arrays.asList("yarra"));
	     while (true) {
	        ConsumerRecords<String, String> records = consumer.poll(100);
	         for (ConsumerRecord<String, String> record : records)
	             System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	         
	     }
	 
	}
	
	
}
