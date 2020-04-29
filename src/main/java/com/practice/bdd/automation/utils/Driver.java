package com.practice.bdd.automation.utils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.ie.InternetExplorerDriver;
import org.openqa.selenium.remote.RemoteWebDriver;


public class Driver {
		
	public static WebDriver driver;
	
	public static WebDriver getInstance() {
		
		if (driver == null || ((RemoteWebDriver) driver).getSessionId() == null){
			
			switch (Config.getProperty("browser")) {
			case "firefox":
				System.setProperty("webdriver.gecko.driver", Config.getProperty("firefox"));
				driver = new FirefoxDriver();
				break;
			case "ie":
				System.setProperty("webdriver.ie.driver", Config.getProperty("ie"));
				driver = new InternetExplorerDriver();
				break;
			default:
				String driverPath = null;
				if(System.getProperty("os.name").contains("Windows")){
					driverPath = System.getProperty("user.dir")+"/"+Config.getProperty("chrome");
				}else{
					driverPath = Config.getProperty("chrome_linux");
				}
				
				driverPath = driverPath.replace("\\", File.separator);
				
				System.setProperty("webdriver.chrome.driver", driverPath);
				
				ChromeOptions options = new ChromeOptions();
//				options.addArguments("--headless");
//				options.setHeadless(true);
//				options.addArguments("window-size=1200x600");
//				options.addArguments("--start-fullscreen");
				options.addArguments("--start-maximized");
				
				if(!System.getProperty("os.name").contains("Windows")){
					options.setBinary(Config.getProperty("chrome_linux"));
				}
				
				Map<String, Object> prefs = new HashMap<String, Object>();
				prefs.put("plugins.plugins_disabled", new String[] { "Chrome PDF Viewer"});
				prefs.put("plugins.always_open_pdf_externally", true);
				prefs.put("download.default_directory", Config.getDownloadLocation());
				prefs.put("profile.default_content_setting_values.notifications", 2);
				prefs.put("credentials_enable_service", false);
				options.setExperimentalOption("prefs", prefs);
				options.addArguments("disable-infobars");

				driver = new ChromeDriver(options);
			}
			driver.manage().deleteAllCookies();
		}
		
		
		
		WebDriverDispatcher disp = new WebDriverDispatcher(driver);
		disp.registerListener(new SeleniumCustomListener());
		
		return disp;
	}
	
	public static void closeDriver() {
		if (driver != null) {
			driver.quit();
			driver=null;
		}
	}
}
