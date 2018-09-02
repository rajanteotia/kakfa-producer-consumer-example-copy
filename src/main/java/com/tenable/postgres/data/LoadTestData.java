package com.tenable.postgres.data;

import com.fasterxml.uuid.Generators;
import com.tenable.kafka.constants.AssetName;
import com.tenable.kafka.constants.Fqdn;
import com.tenable.kafka.constants.Modification;
import com.tenable.kafka.constants.NetBios;
import com.tenable.kafka.constants.OperatingSystem;
import com.tenable.kafka.constants.Protocols;
import com.tenable.kafka.constants.Severity;
import com.tenable.kafka.constants.VulnState;
import com.tenable.kafka.producer.AssetProducerApp;
import com.tenable.kafka.reader.PropertiesReader;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/*
    Utility code to generate bulk data for multiple tables
 */
public class LoadTestData {

	static Long doubleVal = 0l;
	static Long numOfRec = 0l;
	static String output_dir = null;
	static String assetColumnTypes = null;
	static String vulnColumnTypes = null;
	static String jsonData = null;
	static VulnData vulnData = null;
	static Long numOfVulnPerAsset = 0l;
	static boolean isFirstScan = false;
	static Calendar cal = null;
	static Date currentDate = null;
	static int noOfScan = 0;
	static int totalCountOfScans = 0;

	public static void main(String[] args) {
		LoadTestData loadTestData = new LoadTestData();
		vulnData = new VulnData();
		loadTestData.loadProperties();

		for (int i = 0; i < totalCountOfScans;) {
			loadTestData.dataGenerator(loadTestData);
			noOfScan = ++i;
			isFirstScan = false;
		}
	}

	/*
	 * generate data for each column specific to the data type defined in the
	 * properties file
	 */
	public StringBuffer generateDataForColumn(String type, StringBuffer sb, String uuid1, int index) {

		if (type.equalsIgnoreCase("intVal") || type.equalsIgnoreCase("longVal")) {
			sb.append(doubleVal);
			sb.append("|");
		} else if (type.equalsIgnoreCase("plugin_id")) {
			sb.append(generateRandomNumber(100001, 200000));
			sb.append("|");
		} else if (type.equalsIgnoreCase("port")) {
			sb.append(generateRandomNumber(110, 19230));
			sb.append("|");
		} else if (type.contains("dateVal") || type.contains("dateTimeVal")) {
			sb.append(getDate(type, index));
			sb.append("|");
		} else if (type.equalsIgnoreCase("jsonbVal")) {
			sb.append(jsonData);
			sb.append("|");
		} else if (type.equalsIgnoreCase("doubleVal")) {
			sb.append("123443.3432");
			sb.append("|");
		} else if (type.equalsIgnoreCase("boolean")) {
			sb.append("true");
			sb.append("|");
		} else if (type.equalsIgnoreCase("asset_name")) {
			sb.append(AssetName.values()[generateRandomNumber(0, 2)]);
			sb.append("|");
		} else if (type.equalsIgnoreCase("text")) {
			sb.append("general-purpose");
			sb.append("|");
		} else if (type.equalsIgnoreCase("stringList")) {
			sb.append("\"{'192.168.15.117','192.168.15.118'}\"");
			sb.append("|");
		} else if (type.equalsIgnoreCase("ipv4")) {
			sb.append("\"{'192.168.15.117','192.168.15.118'}\"");
			sb.append("|");
		} else if (type.equalsIgnoreCase("ipv6")) {
			sb.append("\"{'2001:DB8:1111:2222::54/64','2001:0:9d38:6ab8:1c48:3a1c:a95a:b1c2'}\"");
			sb.append("|");
		} else if (type.equalsIgnoreCase("fully_qualified_domain_name")) {
			sb.append("\"{'" + Fqdn.values()[generateRandomNumber(0, 21)] + "'}\"");
			sb.append("|");
		} else if (type.equalsIgnoreCase("net_bios")) {
			sb.append(NetBios.values()[generateRandomNumber(0, 42)]);
			sb.append("|");
		} else if (type.equalsIgnoreCase("protocol")) {
			int randomNum = generateRandomNumber(0, 3);
			sb.append(Protocols.values()[randomNum]);
			sb.append("|");
		} else if (type.equalsIgnoreCase("mac_address")) {
			sb.append("\"{'" + randomMACAddress() + "','" + randomMACAddress() + "'}\"");
			sb.append("|");
		} else if (type.equalsIgnoreCase("operating_system")) {
			int randomNum = generateRandomNumber(0, 4);
			sb.append(OperatingSystem.values()[randomNum]);
			sb.append("|");
		} else if (type.equalsIgnoreCase("source")) {
			int randomNum = generateRandomNumber(0, 4);
			sb.append("\"{'" + OperatingSystem.values()[randomNum] + "'}\"");
			sb.append("|");
		} else if (type.equalsIgnoreCase("uuid")) {
			sb.append(uuid1);
			sb.append("|");
		} else if (type.equalsIgnoreCase("enum_vulState")) {
			if (isFirstScan) {
				sb.append(VulnState.NEW);
			} else {
				sb.append(VulnState.values()[generateRandomNumber(1, 2)]);
			}
			sb.append("|");
		} else if (type.equalsIgnoreCase("enum_Modification")) {
			sb.append(Modification.values()[generateRandomNumber(0, 1)]);
			sb.append("|");
		} else if (type.equalsIgnoreCase("enum_severity")) {
			sb.append(Severity.values()[generateRandomNumber(0, 4)]);
			sb.append("|");
		}
		return sb;
	}

	/*
	 * generate random integer within the range for the column
	 */
	private static Integer generateRandomNumber(int min, int max) {
		return ThreadLocalRandom.current().nextInt(min, max);
	}

	/*
	 * generate random MAC address for the column
	 */
	private static String randomMACAddress() {
		Random rand = new Random();
		byte[] macAddr = new byte[6];
		rand.nextBytes(macAddr);

		macAddr[0] = (byte) (macAddr[0] & (byte) 254); // zeroing last 2 bytes to make it unicast and locally
														// adminstrated

		StringBuilder sb = new StringBuilder(18);
		for (byte b : macAddr) {

			if (sb.length() > 0)
				sb.append(":");

			sb.append(String.format("%02x", b));
		}

		return sb.toString();
	}

	/*
	 * write the generated data into the files
	 */
	public void fileWriter(StringBuffer sb, String path) {
		FileOutputStream fos = null;

		try {
			fos = new FileOutputStream(path, true);
			fos.write(sb.toString().getBytes());
			sb.setLength(0);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 * write/read asset IDs from file and write asset data into file
	 */
	public void dataGenerator(LoadTestData loadTestData) {
		String uuid1 = null;
		List<String> deviceIdList = null;
		final String dataTypes[] = assetColumnTypes.split(",");
		StringBuffer sb = new StringBuffer();
		StringBuffer deviceId = new StringBuffer();
		for (int i = 0; numOfRec > i; i++) {
			doubleVal++;
			if (isFirstScan) {
				uuid1 = Generators.timeBasedGenerator().generate().toString();
				deviceId.append(uuid1);
				deviceId.append(System.getProperty("line.separator"));
			} else {
				try {
					deviceIdList = fileReader(output_dir + "/asset_id.csv");
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if(null == deviceIdList && !isFirstScan)
				break;
			String uuid = isFirstScan ? uuid1 : deviceIdList.get(i);
			for (String type : dataTypes) {
				sb = loadTestData.generateDataForColumn(type, sb, uuid, 0);
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append(System.getProperty("line.separator"));

			 AssetProducerApp.runAssetProducer(sb.toString());
			// loadTestData.fileWriter(sb, output_dir +"asset_data" + ".csv");

			vulnData.dataGenerator(numOfVulnPerAsset, vulnColumnTypes, output_dir, uuid);
		}
		if (isFirstScan) {
			loadTestData.fileWriter(deviceId, output_dir + "asset_id" + ".csv");
		}
	}

	/*
	 * load properties from the application.properties file
	 */
	public void loadProperties() {
		Properties prop = PropertiesReader.appPropertyLoader();
		final String dir = System.getProperty("user.dir");
//		prop.load(new FileInputStream(dir + "/application.properties"));
		this.output_dir = prop.getProperty("fileName");
		this.assetColumnTypes = prop.getProperty("assetColumnTypes");
		this.vulnColumnTypes = prop.getProperty("vulnColumnTypes");
		this.numOfRec = Long.parseLong(prop.getProperty("num_of_assets"));
		this.numOfVulnPerAsset = Long.parseLong(prop.getProperty("num_of_vuln_per_asset"));
		this.jsonData = prop.getProperty("jsonData");
		this.isFirstScan = Boolean.parseBoolean(prop.getProperty("isFirstScan"));
		this.noOfScan = Integer.parseInt(prop.getProperty("num_of_scans"));
		this.totalCountOfScans = Integer.parseInt(prop.getProperty("total_count_of_scans"));
			
		cal = Calendar.getInstance();
		cal.set(2017, 00, 01, 00, 00, 00);
		currentDate = new Date();
	}

	/*
	 * get the date generated as expected in the column
	 */
	public String getDate(String colName, int index) {
		String[] val = colName.split("_");
		if (isFirstScan) {
			if (val[1].equalsIgnoreCase("createdDate") || val[1].equalsIgnoreCase("updatedDate")
					|| val[1].equalsIgnoreCase("firstScanTime")) {
				return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'").format(addDays(cal.getTime(), 0));
			} else if (val[1].equalsIgnoreCase("firstSeen")) {
				return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'").format(addMin(cal.getTime(), 1));
			} else if (val[1].equalsIgnoreCase("lastSeen")) {
				return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'").format(addMin(cal.getTime(), 1 + index));
			} else if (val[1].equalsIgnoreCase("lastScanTime") || val[1].equalsIgnoreCase("lastAuthenticatedScanTime")
					|| val[1].equalsIgnoreCase("lastAuthenticatedScanDate")
					|| val[1].equalsIgnoreCase("lastUnauthenticatedScanDate")) {
				return "";
			} else if (val[1].equalsIgnoreCase("firstFixed") || val[1].equalsIgnoreCase("lastFixed")) {
				return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'")
						.format(addDays(addMin(cal.getTime(), 1 + index), 1));
			}
		} else {
			int numDaysSkip = noOfScan * 3;
			if (val[1].equalsIgnoreCase("createdDate") || val[1].equalsIgnoreCase("updatedDate")) {
				return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'").format(addDays(cal.getTime(), numDaysSkip));
			} else if (val[1].equalsIgnoreCase("lastAuthenticatedScanTime")
					|| val[1].equalsIgnoreCase("lastScanTime")) {
				return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'")
						.format(minusDays(addDays(cal.getTime(), numDaysSkip), 3));
			} else if (val[1].equalsIgnoreCase("lastAuthenticatedScanDate")
					|| val[1].equalsIgnoreCase("lastUnauthenticatedScanDate")) {
				return "\"" + new SimpleDateFormat("MMMM dd, yyyy")
						.format(minusDays(addDays(cal.getTime(), numDaysSkip), 3)) + "\"";
			} else if (val[1].equalsIgnoreCase("firstScanTime")) {
				return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'").format(minusDays(cal.getTime(), 0));
			} else if (val[1].equalsIgnoreCase("firstSeen")) {
				return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'").format(addMin(cal.getTime(), 1 + index));
			} else if (val[1].equalsIgnoreCase("lastSeen")) {
				return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'")
						.format(addDays(addMin(cal.getTime(), 1 + index), numDaysSkip));
			} else if (val[1].equalsIgnoreCase("firstFixed") || val[1].equalsIgnoreCase("lastFixed")) {
				return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'")
						.format(addDays(addMin(cal.getTime(), 1 + index), numDaysSkip + 1));
			}
		}
		return "";
	}

	/*
	 * read the file
	 */
	public List<String> fileReader(String path) throws FileNotFoundException {
		List<String> deviceIdList = new ArrayList<>();
		FileReader fileReader = null;
		try {
			File file = new File(path);
			fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				deviceIdList.add(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fileReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return deviceIdList;
	}

	public static Date addDays(Date date, int days) {
		GregorianCalendar cal = new GregorianCalendar();
		cal.setTime(date);
		cal.add(Calendar.DATE, days);
		return cal.getTime();
	}

	public static Date minusDays(Date date, int days) {
		GregorianCalendar cal = new GregorianCalendar();
		cal.setTime(date);
		cal.add(Calendar.DATE, -days);
		return cal.getTime();
	}

	public static Date addMin(Date date, int min) {
		GregorianCalendar cal = new GregorianCalendar();
		cal.setTime(date);
		cal.add(Calendar.MINUTE, min);
		return cal.getTime();
	}
	/*
	 * public static Date minusMin(Date date, int min) { GregorianCalendar cal = new
	 * GregorianCalendar(); cal.setTime(date); cal.add(Calendar.MINUTE, -min);
	 * return cal.getTime(); }
	 */
}
