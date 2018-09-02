package com.tenable.kafka.consumer;

import com.tenable.kafka.constants.IKafkaConstants;
import com.tenable.kafka.database.VulnDatabase;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class VulnConsumerApp {

    public static void main(String[] args) {
        runConsumer();
    }

    static void runConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer(IKafkaConstants.VULN_INSTANCE_TOPIC_NAME, IKafkaConstants.GROUP_10_CONFIG);

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
                
                VulnDatabase.insertData(record.value());
            });
            consumer.commitAsync();
        }
//        consumer.close();
    }

}
