package com.tenable.kafka.constants;

public interface IKafkaConstants {
	public static String KAFKA_BROKERS = "localhost:9092";
	
	public static Integer MESSAGE_COUNT=10;
	
	public static String CLIENT_ID="client1";
	
	public static String ASSET_TOPIC_NAME="asset_data";

	public static String VULN_INSTANCE_TOPIC_NAME="vuln_instance_data";

	public static String GROUP_9_CONFIG="consumerGroup09";

	public static String GROUP_10_CONFIG="consumerGroup10";
	
	public static Integer MAX_NO_MESSAGE_FOUND_COUNT=100;
	
	public static String OFFSET_RESET_LATEST="latest";
	
	public static String OFFSET_RESET_EARLIER="earliest";
	
	public static Integer MAX_POLL_RECORDS=1;

	public static String ASSET_FILE_PATH = "C:\\Users\\rajan\\Downloads\\LoadTestData\\asset_data.csv";

	public static String VULN_INSTANCE_FILE_PATH = "C:\\Users\\rajan\\Downloads\\LoadTestData\\vuln_instance_data.csv";

}
