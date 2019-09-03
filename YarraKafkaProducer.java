 package com.yarra.spark.kafka.kerberos;

 import java.util.Enumeration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;


 public class YarraKafkaProducer {

     private static Producer producer = null;
     private YarraKafkaProducer() {
     }

     public static Producer getProducer(String bootstrap_servers) {
         if (producer == null) {
         	 System.setProperty("java.security.auth.login.config","/opt/spark/mnt/jaas/kafka_client_withoutcache_jaas.conf");
         	 System.setProperty("java.security.krb5.conf", "/opt/spark/mnt/krb5/krb5.conf");
        	 System.setProperty("sun.security.krb5.debug", "false");
             Properties props = new Properties();
             props.put("bootstrap.servers", bootstrap_servers);
             props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
             props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
             props.put("sasl.kerberos.service.name", "yarra");
             props.put("sasl.mechanism", "GSSAPI");
             props.put("client.id", "YARRAP");
             //non SSL
             //props.put("security.protocol", "SASL_PLAINTEXT");
             // SSL properties
             props.put("security.protocol", "SASL_SSL");
             props.put("ssl.truststore.location", "/opt/spark/mnt/truststore/kafka.client.truststore.jks");
             props.put("ssl.truststore.password", "clientpass");
             
             Properties p = System.getProperties();
             Enumeration keys = p.keys();
             System.out.println("********************** JVM properties ********************* " );
             while (keys.hasMoreElements()) {
                 String key = (String)keys.nextElement();
                 String value = (String)p.get(key);
                 System.out.println(key + ": " + value);
             }
             System.out.println("********************** JVM properties printing is over ********************* " );
             producer = new KafkaProducer(props);

         }
         return producer;
     }
 }