package com.practice.bdd.automation.runners;





import org.testng.annotations.Test;

import cucumber.api.CucumberOptions;
	import cucumber.api.testng.AbstractTestNGCucumberTests;

	@CucumberOptions(
			plugin = {"json:target/cucumber-report.json", "html:target/cucumber-report", "com.cucumber.listener.ExtentCucumberFormatter:"},
			features={"src/test/resources"},
			glue={"com.practice.bdd.automation.stepdefinitions"},
			tags={"@test_practice"}
		    ,dryRun=true
	)
	@Test
	public class PracticeDry_Runner  extends AbstractTestNGCucumberTests{
		
		
	    
	    
}
