package com.tenable.kafka.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataFileReader {

    /*
        read the file for data
    */
    public List<String> fileReader(String path) {
        List<String> deviceIdList = new ArrayList<>();
        java.io.FileReader fileReader = null;
        try {
            File file = new File(path);
            fileReader = new java.io.FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                deviceIdList.add(line);
            }
        }  catch (FileNotFoundException e) {
            System.out.println("Error in reading the file");
            System.out.println(e);
        } catch (IOException e) {
            System.out.println("IOException in reading the file");
            System.out.println(e);
        } finally {
            try {
                fileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return  deviceIdList;
    }
}
