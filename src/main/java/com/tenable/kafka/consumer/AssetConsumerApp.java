package com.tenable.kafka.consumer;

import com.tenable.kafka.constants.IKafkaConstants;
import com.tenable.kafka.database.AssetDatabase;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class AssetConsumerApp {

    public static void main(String[] args) {
        runConsumer();
    }

    static void runConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer(IKafkaConstants.ASSET_TOPIC_NAME, IKafkaConstants.GROUP_9_CONFIG);

        int noMessageToFetch = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1);
//            if (consumerRecords.count() == 0) {
//                noMessageToFetch++;
//                if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
//                    break;
//                else
//                    continue;
//            }

            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());

                AssetDatabase.insertData(record.value());                

            });
            consumer.commitAsync();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
                System.out.println(ie);
            }
        }
//        consumer.close();
    }

}
