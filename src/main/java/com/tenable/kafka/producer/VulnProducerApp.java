package com.tenable.kafka.producer;

import com.tenable.kafka.constants.IKafkaConstants;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.*;
import java.util.concurrent.ExecutionException;

public class VulnProducerApp {

//    public static void main(String[] args) {
//        runProducer();
////		runConsumer();
//    }
    static int index = 0;
    public static void runVulnProducer(String data) {
        Producer<Long, String> producer = ProducerCreator.createProducer();


        try {

            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.VULN_INSTANCE_TOPIC_NAME, 
            		Long.valueOf(index),data);
            RecordMetadata metadata = producer.send(record).get();
//				RecordMetadata metadata = producer.send(record);
            System.out.println("This vuln data is " + data);
//            System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
//                    + " with offset " + metadata.offset());
            Thread.sleep(1000);

            index++;

        } catch (ExecutionException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
}
