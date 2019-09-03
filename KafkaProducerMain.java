package com.yarra.spark.kafka.kerberos;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerMain {

	public static void main(String[] args) {
		System.setProperty("java.security.auth.login.config","/opt/spark/mnt/jaas/kafka_client_withoutcache_jaas.conf");
		System.setProperty("java.security.krb5.conf", "/opt/spark/mnt/krb5/krb5.conf");
		System.setProperty("sun.security.krb5.debug", "false");
		Producer<String, String> producer =  null;
		try {
			
			String bootstrap_servers = args[0]; 
			Properties props = new Properties();

			props.put("bootstrap.servers", bootstrap_servers);
			 props.put("acks", "all");
			props.put("serializer.class", "kafka.serializer.StringEncoder");
		    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.kerberos.service.name", "yarra");
            props.put("sasl.mechanism", "GSSAPI");
            props.put("client.id", "YARRAP");
    
            producer = new KafkaProducer(props);
             while(true) {
				for(int i = 0; i<10000;i++) {
					producer.send(new ProducerRecord<String, String>("yarra", String.valueOf(i),String.valueOf(i)));
				}
				break;
			}

		} catch (Exception e) {
			e.printStackTrace();
			
		} finally {
			if (producer != null) {
				producer.close();
			}
		}

	}

}
