package com.yejunyu.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author yejunyu
 * @date 18-7-16.
 */
public class MyProducer extends Thread {

    private String topic;

    private KafkaProducer<Integer, String> producer;

    public MyProducer(String topic) {
        this.topic = topic;

        Properties props = new Properties();

        props.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<Integer, String>(props);
    }

    @Override
    public void run() {
        int mcnt = 0;
        while (true) {
            producer.send(new ProducerRecord<Integer, String>(topic, "hello " + mcnt));
            mcnt++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}
