package com.github.atif.kafka.basic;

import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.util.Properties;
public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        Logger logger = (Logger) LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrapServers="127.0.0.1:9092";
        // Create Producer properties
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create The Producer
        KafkaProducer<String, String> producer= new KafkaProducer<>(properties);

        // Create  producer record
        for(int i=0;i<10;i++) {

            ProducerRecord<String, String> record= new ProducerRecord<>("first_topic","hello world "+Integer.toString(i));

            // Send Data  = asynchrnous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        // the record was successfully send
                        logger.info("Received new metadata. \n"+
                                "Topic: "+recordMetadata.topic()+ "\n"+
                                "Partition: "+recordMetadata.partition()+ "\n"+
                                "Offsets: "+recordMetadata.offset()+ "\n"+
                                "Timestamp: "+recordMetadata.timestamp()
                        );
                    }
                    else{
                        logger.error("Error while producing ",e);
                    }
                }
            });

        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
