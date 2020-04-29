package com.practice.bdd.automation.utils;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class Config {
	private static Properties configFile;

	public static void EstablishFileStream(String fileName) {
		try {
			String path = "./config/" + fileName + ".properties";
			FileInputStream input = new FileInputStream(path);

			configFile = new Properties();
			configFile.load(input);

			input.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String getProperty(String keyName) {
		String fileName = "configuration";
		EstablishFileStream(fileName);
		return configFile.getProperty(keyName);
	}

	public static String getProperty(String keyName, String fileName) {
		EstablishFileStream(fileName);
		return configFile.getProperty(keyName);
	}

	public static String getProjectPath() {
		return System.getProperty("user.dir");
	}
	
	public static String getDownloadLocation(){
		String location = null;
		if(null != getProperty("downloadLocation")){
			location = System.getProperty("user.dir")+"/"+getProperty("downloadLocation");
		}else{
			location = System.getProperty("user.home")+"/"+"Downloads";
		}
		 
		return location.replace("/", File.separator);
	}

}
