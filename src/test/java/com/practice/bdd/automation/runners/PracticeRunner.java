
package com.practice.bdd.automation.runners;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.cucumber.listener.ExtentProperties;
import com.cucumber.listener.Reporter;

import cucumber.api.CucumberOptions;
import cucumber.api.testng.AbstractTestNGCucumberTests;

@CucumberOptions(
		plugin = {"json:target/cucumber-report.json", "html:target/cucumber-report", "com.cucumber.listener.ExtentCucumberFormatter:"},
		features={"src/test/resources"},
		glue={"com.practice.bdd.automation.stepdefinitions"},
		tags={"@test_practice"},monochrome = true

)


@Test
public class PracticeRunner extends AbstractTestNGCucumberTests{
	
	@BeforeClass
	public void reportSetup() throws IOException{
		ExtentProperties extentProperties = ExtentProperties.INSTANCE;
		String reportPath = "TestResult/Report_" +new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new Date())+".html";
        extentProperties.setReportPath(reportPath);
        System.out.println("Execution started");    
	}
	
	@AfterClass
	public void teardown() {
	
        Reporter.setSystemInfo("user", System.getProperty("user.name"));
        if(System.getProperty("os.name").contains("Windows")){
        	Reporter.setSystemInfo("os", "Windows");
		}else{
			Reporter.setSystemInfo("os", "Linux");
		}   
    }
    
   
}
