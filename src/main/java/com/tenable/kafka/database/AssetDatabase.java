package com.tenable.kafka.database;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;
import com.tenable.kafka.reader.PropertiesReader;

public class AssetDatabase {

    private final static String DATE_WITH_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'";

    public Connection connect() {
        Connection conn = null;
        try {
        	PropertiesReader.dbPropertyLoader();
        	Class.forName(PropertiesReader.DB_DRIVER);
            conn = DriverManager.getConnection(PropertiesReader.DB_URL, PropertiesReader.DB_USERNAME, PropertiesReader.DB_PASSWORD);
            System.out.println("Connected to the PostgreSQL server successfully.");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }

    public void insertAsset(Connection connection,String assetId,String containerId,String type,Date createdDate,Date updatedDate,
    		Date firstScanTime,Date lastScanTime,Date lastAuthenticatedScanTime,
    		Array ipv4,Array ipv6,Array fqdn,String netBios,Array macAddress,String operatingSystem,String biosUuid,
    		String tenableUuid,String serviceNowSysId,Array source,String name,Date lastAuthenticatedScanDate,
    		Date lastUnauthenticatedScanDate) {
    	
    	String sql = "INSERT INTO asset (id,container_id,type,created_date,updated_date,first_scan_time,last_scan_time,"
    			+ "last_authenticated_scan_time,ipv4,ipv6,fqdn,netbios,mac_address,operating_system,bios_uuid,tenable_uuid,"
    			+ "servicenow_sysid,source,name,last_authenticated_scan_date,last_unauthenticated_scan_date) "
    			+ "VALUES (?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?,?)";
    	System.out.println(sql);
        try {
            PreparedStatement st = connection.prepareStatement(sql);
            st.setObject(1, assetId, Types.OTHER);
            st.setObject(2, containerId, Types.OTHER);
            st.setString(3, type);
            st.setDate(4, createdDate);
            st.setDate(5, updatedDate);
            st.setDate(6, firstScanTime);
            st.setDate(7, lastScanTime);
            st.setDate(8, lastAuthenticatedScanTime);
            st.setArray(9, ipv4);
            st.setArray(10, ipv6);
            st.setArray(11, fqdn);
            st.setString(12, netBios);
            st.setArray(13, macAddress);
            st.setString(14, operatingSystem);
            st.setString(15, biosUuid);
            st.setObject(16, tenableUuid, Types.OTHER);
            st.setString(17, serviceNowSysId);
            st.setArray(18, source);
            st.setString(19, name);
            st.setDate(20, lastAuthenticatedScanDate);
            st.setDate(21, lastUnauthenticatedScanDate);
            
            st.executeUpdate();
            st.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        System.out.println("\n\nData inserted successfully\n\n");
    }

    public static void insertData(String dataRow) {
        AssetDatabase database = new AssetDatabase();
        Connection connection = database.connect();        
        
        Array arrayIpv4 = null;
        Array arrayIpv6 = null;
        Array arrayFqdn = null;
        Array arrayMacAddress = null;
        Array arraySource = null;
        Date createdDate = null;
        Date updatedDate = null;
        Date firstScanTime = null;
        Date lastScanTime = null;
        Date lastAuthenticatedScanTime = null;
        Date lastAuthenticatedScanDate = null;
        Date lastUnauthenticatedScanDate = null;
        
        String row[] = dataRow.split(Pattern.quote("|"));

        String asset_id = row[0];
        String container_id = row[1];
        String type = row[2];
        String created_date = row[3];
        String updated_date = row[4];
        String first_scan_time = row[5];
        String last_scan_time = row[6];
        String last_authenticated_scan_time = row[7];
        String ipv4 = row[8];
        String ipv6 = row[9];
        String fqdn = row[10];
        String netbios = row[11];
        String mac_address = row[12];
        String operating_system = row[13];
        String bios_uuid = row[14];
        String tenable_uuid = row[15];
        String servicenow_sysid = row[16];
        String source = row[17];
        String name = row[18];
        String last_authenticated_scan_date = row[19];
        String last_unauthenticated_scan_date = row[20];

        for(int i=0; i< row.length;i++)
        	System.out.println(row[i]);
        
        try {
			arrayIpv4 = connection.createArrayOf("text", ipv4.split(""));
			arrayIpv6 = connection.createArrayOf("text", ipv6.split(""));
			arrayFqdn = connection.createArrayOf("text", fqdn.split(""));
			arrayMacAddress = connection.createArrayOf("text", mac_address.split(""));
			arraySource = connection.createArrayOf("text", source.split(""));
			
			createdDate = new Date(new SimpleDateFormat(DATE_WITH_TIME_FORMAT).parse(created_date).getTime());
	        
	        updatedDate = new Date(new SimpleDateFormat(DATE_WITH_TIME_FORMAT).parse(updated_date).getTime());
	        firstScanTime = new Date(new SimpleDateFormat(DATE_WITH_TIME_FORMAT).parse(first_scan_time).getTime());

	        if(null != last_scan_time && !last_scan_time.isEmpty())
	        	lastScanTime = new Date(new SimpleDateFormat(DATE_WITH_TIME_FORMAT).parse(last_scan_time).getTime());
	        if(last_authenticated_scan_time != null && !last_authenticated_scan_time.isEmpty())
	        	lastAuthenticatedScanTime = new Date(new SimpleDateFormat(DATE_WITH_TIME_FORMAT).parse(last_authenticated_scan_time).getTime());
	        if(last_authenticated_scan_date != null && !last_authenticated_scan_date.isEmpty())
	        	lastAuthenticatedScanDate = new Date(new SimpleDateFormat("MMMM dd, yyyy").parse(last_authenticated_scan_date.replace("\"", "")).getTime());
	        if(last_unauthenticated_scan_date != null && !last_unauthenticated_scan_date.isEmpty() && last_unauthenticated_scan_date == "\n")
	        	lastUnauthenticatedScanDate = new Date(new SimpleDateFormat("MMMM dd, yyyy").parse(last_unauthenticated_scan_date.replace("\"", "")).getTime());
	        
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

        database.insertAsset(connection,asset_id,container_id,type,createdDate,updatedDate,firstScanTime,lastScanTime,
        		lastAuthenticatedScanTime,arrayIpv4,arrayIpv6,arrayFqdn,netbios,arrayMacAddress,operating_system,
        		bios_uuid,tenable_uuid,servicenow_sysid,arraySource,name,lastAuthenticatedScanDate,
        		lastUnauthenticatedScanDate);
    }
}