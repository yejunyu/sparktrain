package com.yejunyu.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author yejunyu
 * @date 18-7-20.
 */
public class MyConsumer extends Thread {
    Consumer<String, String> consumer;
    private String topic;

    public MyConsumer(String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        props.put("group.id", "test");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList(this.topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
