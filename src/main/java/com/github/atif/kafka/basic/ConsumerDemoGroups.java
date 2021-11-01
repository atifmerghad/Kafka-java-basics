package com.github.atif.kafka.basic;

import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {

        Logger logger = (Logger) LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrapServers="127.0.0.1:9092";
        String groupId="my-fifth-application";
        String topic = "first_topic";

        // Create Consumer configs
        Properties properties= new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe to a topic(s)
        consumer.subscribe(Arrays.asList("first_topic")); //(Collections.singleton(topic)); // Arrays.asList("first_topic", "second_topic")

        // poll for new data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records){
                logger.info("Key: "+record.key()+", Value: " + record.value());
                logger.info("Partition: "+ record.partition()+ ", Offset: "+record.offset());
            }
        }
    }
}
