package com.practice.bdd.automation.pages;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

import com.practice.bdd.automation.utils.Driver;

public class LoginPage {
	WebDriver driver;
	
	public LoginPage() {
		this.driver = Driver.getInstance();
		PageFactory.initElements(driver, this);
	}
	
	@FindBy(xpath="(//input[@type='text'])[2]")
	public WebElement  unTxt;
	
	@FindBy(xpath="")
	public WebElement pwdTxt;
	
	@FindBy(xpath="") 
	public WebElement loginBtn;
	

}
