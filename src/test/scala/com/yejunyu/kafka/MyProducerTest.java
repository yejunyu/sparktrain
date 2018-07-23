package com.yejunyu.kafka;

/**
 * @author yejunyu
 * @date 18-7-20.
 */
public class MyProducerTest {
    public static void main(String[] args) {
        new MyProducer("hello").start();
    }

}
