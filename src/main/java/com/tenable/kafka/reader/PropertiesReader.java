package com.tenable.kafka.reader;

import java.util.Properties;

import org.apache.logging.log4j.util.PropertiesUtil;

public class PropertiesReader {

	public static String DB_DRIVER = null;
	public static String DB_URL = null;
	public static String DB_USERNAME = null;
	public static String DB_PASSWORD = null;
	
	public static void dbPropertyLoader() {
        Properties properties = null;
        if (properties == null) {
            properties = new Properties();
            try {
                properties.load(PropertiesUtil.class
                        .getResourceAsStream("/postgres_config.properties"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        DB_DRIVER = properties.getProperty("jdbc.driver");
        DB_URL = properties.getProperty("jdbc.url");
        DB_USERNAME = properties.getProperty("jdbc.username");
        DB_PASSWORD = properties.getProperty("jdbc.password");        
    }
	
	public static Properties appPropertyLoader() {
        Properties properties = null;
        if (properties == null) {
            properties = new Properties();
            try {
                properties.load(PropertiesUtil.class
                        .getResourceAsStream("/application.properties"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return properties;
    }

}
