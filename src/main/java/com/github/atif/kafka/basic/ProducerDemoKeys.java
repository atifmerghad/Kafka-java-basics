package com.github.atif.kafka.basic;

import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = (Logger) LoggerFactory.getLogger(ProducerDemoKeys.class);
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

            String topic = "first_topic";
            String value = "hello world "+Integer.toString(i);
            String key = "id_"+Integer.toString(i);

            ProducerRecord<String, String> record= new ProducerRecord<>(topic,key,value);
            logger.info("Key: "+key); // log the key
            // id_0 is going to partition 1
            // id_1 partition 0


            // Send Data  = asynchrnous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or exception is thrown
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
            }).get(); // Block the .send() to make it synchronous - don't do this in production!

        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
