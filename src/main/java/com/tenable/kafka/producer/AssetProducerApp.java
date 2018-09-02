package com.tenable.kafka.producer;

import com.tenable.kafka.constants.IKafkaConstants;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.*;
import java.util.concurrent.ExecutionException;

public class AssetProducerApp {

    static int index = 0;
    public static void runAssetProducer(String data) {
        Producer<Long, String> producer = ProducerCreator.createProducer();

        try {

            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.ASSET_TOPIC_NAME, 
            		Long.valueOf(index), data);
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("This asset data is " + data);
//                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
//                        + " with offset " + metadata.offset());
            Thread.sleep(1000);
            index++;

        } catch (ExecutionException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
            System.out.println("Error in sending record");
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }

}
