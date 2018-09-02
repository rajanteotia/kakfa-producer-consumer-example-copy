package com.tenable.kafka.database;

import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.logging.log4j.util.PropertiesUtil;

import com.fasterxml.uuid.Generators;
import com.tenable.kafka.constants.Modification;
import com.tenable.kafka.constants.Severity;
import com.tenable.kafka.constants.VulnState;
import com.tenable.kafka.reader.PropertiesReader;

public class VulnDatabase {

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
    
    
    public void insertVuln(Connection connection,String assetId,Integer pluginId,Integer port,String protocol,VulnState vulnState,
    		String pluginOutput,Modification modification,Date firstSeen, Date lastSeen,Date firstFixed,Date lastFixed,
    		String containerId,Severity severity) {
    	
    	String sql = "INSERT INTO vuln_instance (asset_id,plugin_id,port,protocol,vuln_state,plugin_output,modification,"
    			+ "first_seen,last_seen,first_fixed,last_fixed,container_id,severity) "
    			+ "VALUES (?,?,?,?,CAST(? AS vulnstate),?,CAST(? AS modification),?,?,?,?,?,CAST(? AS severity))";
    	System.out.println(sql);
        try {
            PreparedStatement st = connection.prepareStatement(sql);
            st.setObject(1, assetId, Types.OTHER);
            st.setInt(2, pluginId);
            st.setInt(3, port);
            st.setString(4, protocol);
            st.setString(5, vulnState.toString());
            st.setString(6, pluginOutput);
            st.setString(7, modification.toString());
            st.setDate(8, firstSeen);
            st.setDate(9, lastSeen);
            st.setDate(10, firstFixed);
            st.setDate(11, lastFixed);
            st.setObject(12, containerId, Types.OTHER);
            st.setString(13, severity.toString());
            
            st.executeUpdate();
            st.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void insertData(String dataRow) {
        VulnDatabase database = new VulnDatabase();
        Connection connection = database.connect();        
        
        Date firstSeen = null;
        Date lastSeen = null;
        Date firstFixed = null;
        Date lastFixed = null;
        
        String row[] = dataRow.split(Pattern.quote("|"));

        for(int i=0; i< row.length;i++)
        	System.out.println("row["+i+"] = "+row[i]);
        
        String asset_id = row[0];
        Integer plugin_id = Integer.parseInt(row[1]);
        Integer port = Integer.parseInt(row[2]);
        String protocol = row[3];
        VulnState vuln_state = VulnState.valueOf(row[4]);
        String plugin_output = row[5];
        Modification modification = Modification.valueOf(row[6]);
        String first_seen = row[7];
        String last_seen = row[8];
        String first_fixed = row[9];
        String last_fixed = row[10];
        String container_id = row[11];
    	System.out.println("severity = " +row[12]);
        Severity severity = Severity.valueOf(row[12].replace("\n", ""));

        try {			
	        if(null != first_seen && !first_seen.isEmpty())
	        	firstSeen = new Date(new SimpleDateFormat(DATE_WITH_TIME_FORMAT).parse(first_seen).getTime());
	        if(last_seen != null && !last_seen.isEmpty())
	        	lastSeen = new Date(new SimpleDateFormat(DATE_WITH_TIME_FORMAT).parse(last_seen).getTime());
	        if(first_fixed != null && !first_fixed.isEmpty())
	        	firstFixed = new Date(new SimpleDateFormat(DATE_WITH_TIME_FORMAT).parse(first_fixed).getTime());
	        if(last_fixed != null && !last_fixed.isEmpty())
	        	lastFixed = new Date(new SimpleDateFormat(DATE_WITH_TIME_FORMAT).parse(last_fixed).getTime());
	        
		} catch (Exception e) {
			e.printStackTrace();
		}

        database.insertVuln(connection,asset_id,plugin_id,port,protocol,vuln_state,plugin_output,modification,
        		firstSeen,lastSeen,firstFixed,lastFixed,container_id,severity);
    }
}