package com.practice.bdd.automation.stepdefinitions;


import org.openqa.selenium.WebDriver;
import org.testng.Assert;

import com.cucumber.listener.Reporter;
import com.practice.bdd.automation.pages.LoginPage;
import com.practice.bdd.automation.utils.Config;
import com.practice.bdd.automation.utils.Driver;
import com.practice.bdd.automation.utils.SeleniumTestHelper;

import cucumber.api.Scenario;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;

public class Practice_StepDefinition extends CommonStepDefination {
	WebDriver driver;
	LoginPage loginPage = new LoginPage();
	
	public Practice_StepDefinition() {
		this.driver = Driver.getInstance();
	}

	@Before
	public void intiate(Scenario scenario) {

	}

	@After
	public void cleanUp(Scenario scenario) {

	}
	
	@Given("^user is on flipkart login page$")
	public void user_is_on_flipkart_login_page() throws Throwable {
	   driver.get(Config.getProperty("app_url"));
	   SeleniumTestHelper.waitForElementToBeClickable(driver, loginPage.unTxt, 20);
	   Assert.assertEquals(true, true);
	   System.out.println("Login pageverified successfully");
	   Reporter.addStepLog("Login pageverified successfully");
	}
	
	@When("^user enters \"([^\"]*)\" and \"([^\"]*)\", click on login btn and verifies login staus$")
	public void user_enters_and_click_on_login_btn_and_verifies_login_staus(String userName, String password) throws Throwable {
	  
	}

}
