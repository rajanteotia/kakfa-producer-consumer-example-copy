package com.tenable.kafka;

import java.io.*;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.tenable.kafka.constants.IKafkaConstants;
import com.tenable.kafka.consumer.ConsumerCreator;
import com.tenable.kafka.producer.ProducerCreator;

public class App {
	public static void main(String[] args) {
		runProducer();
//		runConsumer();
	}

	static void runConsumer() {
		Consumer<Long, String> consumer = ConsumerCreator.createConsumer("demo", "demo");

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(100);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.println("Record Key " + record.key());
				System.out.println("Record value " + record.value());
				System.out.println("Record partition " + record.partition());
				System.out.println("Record offset " + record.offset());
			});
			consumer.commitAsync();
		}
		consumer.close();
	}

	static void runProducer() {
		Producer<Long, String> producer = ProducerCreator.createProducer();
//		List<String> deviceIdList = new DataFileReader().fileReader("C:\\Users\\rajan\\Downloads\\LoadTestData\\asset_id.csv");
		FileReader fileReader = null;
		int index = 0;
		while(true) {

//			final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, Long.valueOf(index),
//					"This asset ID is " + deviceIdList.get(index));
			try {
				File file = new File(IKafkaConstants.ASSET_FILE_PATH);
				fileReader = new FileReader(file);
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.ASSET_TOPIC_NAME, Long.valueOf(index),
							"This asset ID is " + line);
					RecordMetadata metadata = producer.send(record).get();
//				RecordMetadata metadata = producer.send(record);
					System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
							+ " with offset " + metadata.offset());
					Thread.sleep(1000);
				}
				index++;

			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
				System.out.println("Error in sending record");
			} catch (InterruptedException e) {
				System.out.println(e);
			} catch (FileNotFoundException e) {
				System.out.println("Error in reading the file");
				System.out.println(e);
			} catch (IOException e) {
				System.out.println("IOException in reading the file");
				System.out.println(e);
			}
		}
	}
}
