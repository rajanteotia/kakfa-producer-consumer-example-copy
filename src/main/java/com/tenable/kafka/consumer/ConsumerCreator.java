package com.tenable.kafka.consumer;

import java.util.Collections;
import java.util.Properties;

import com.tenable.kafka.constants.IKafkaConstants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerCreator {

	public static Consumer<Long, String> createConsumer(String topic, String groupIdConfig) {
		if(!topic.isEmpty() && topic != null) {
			final Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
			props.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdConfig);
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, IKafkaConstants.MAX_POLL_RECORDS);
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_EARLIER);

			final Consumer<Long, String> consumer = new KafkaConsumer<>(props);
			consumer.subscribe(Collections.singletonList(topic));
			return consumer;
		}
		return null;
	}

}
