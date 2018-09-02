package com.tenable.postgres.data;

import com.tenable.kafka.producer.VulnProducerApp;

/*
    Vuln Instance data generation & write
*/
public class VulnData {

    static Long doubleVal=0L;
    private static LoadTestData loadTestData=new LoadTestData();


    /*
        generate vuln instance data and write to file
     */
    public void dataGenerator(Long val_count_per_asset, String vulnColumnTypes, String path, String uuid1) {
        final String dataTypes[] = vulnColumnTypes.split(",");
        StringBuffer sbVuln = new StringBuffer();
        for (int i = 0; val_count_per_asset > i; i++) {
            doubleVal++;
            for (String type : dataTypes) {
                sbVuln = loadTestData.generateDataForColumn(type, sbVuln,uuid1, i);
            }
            sbVuln.append(System.getProperty("line.separator"));
            // push the generated vuln instance data into the kafka topic
            VulnProducerApp.runVulnProducer(sbVuln.toString());
        }
//        loadTestData.fileWriter(sbVuln,path+"vuln_instance_data"+".csv");
    }
}
