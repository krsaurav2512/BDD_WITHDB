package com.practice.bdd.automation.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.openqa.selenium.By;
import org.openqa.selenium.Dimension;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.Point;
import org.openqa.selenium.Rectangle;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebDriverException;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.ie.InternetExplorerDriver;
import org.openqa.selenium.support.events.WebDriverEventListener;

import com.practice.bdd.automation.utils.SeleniumCustomListener.Operations;

public class WebDriverDispatcher implements WebDriver, WebElement,
		JavascriptExecutor {

	private WebDriver driver;
	private WebElement currentElement;
	private By by;
	private List<WebElement> currentElements;

	List<WebDriverEventListener> listeners;

	public WebDriverDispatcher(WebDriver driver) {
		this.driver = driver;
		listeners = new ArrayList<>();
	}

	public void registerListener(WebDriverEventListener listener) {
		listeners.add(listener);
	}

	// Screenshot Method
	@Override
	public <X> X getScreenshotAs(OutputType<X> target)
			throws WebDriverException {

		switch (Config.getProperty("browser")) {
		case "firefox":
			return ((FirefoxDriver) driver).getScreenshotAs(target);
		case "ie":
			return ((InternetExplorerDriver) driver).getScreenshotAs(target);
		default:
			return ((ChromeDriver) driver).getScreenshotAs(target);
		}

	}

	@Override
	public List<WebElement> findElements(By by) {

		for (WebDriverEventListener l : listeners) {
			SeleniumCustomListener.operation = Operations.FIND_ELEMENTS;
			l.beforeFindBy(by, currentElement, driver);
		}

		currentElements = driver.findElements(by);

		for (WebDriverEventListener l : listeners) {
			l.afterFindBy(by, currentElement, driver);
		}

		return currentElements;
	}

	@Override
	public WebElement findElement(By by) {
		for (WebDriverEventListener l : listeners) {
			SeleniumCustomListener.operation = Operations.FIND_ELEMENT;
			l.beforeFindBy(by, currentElement, driver);
		}

		long start = System.currentTimeMillis();
		while (true) {
			try {
				currentElement = driver.findElement(by);
				break;
			} catch (NoSuchElementException e) {
				long end = System.currentTimeMillis();
				if (end - start > 15000) {
					throw e;
				}
				sleep(1);
			}
		}

		for (WebDriverEventListener l : listeners) {
			l.afterFindBy(by, currentElement, driver);
		}
		this.by = by;
		return this;
	}
	
	public boolean verifyElement(By by, int timeout) {
		boolean isElementFound = true;
		for (WebDriverEventListener l : listeners) {
			SeleniumCustomListener.operation = Operations.VERIFY_ELEMENT;
			SeleniumCustomListener.externalTimeout = timeout;
			l.beforeFindBy(by, currentElement, driver);
			SeleniumCustomListener.externalTimeout = 0;
		}

		long start = System.currentTimeMillis();
		while (true) {
			try {
				currentElement = driver.findElement(by);
				isElementFound = true;
				break;
			} catch (NoSuchElementException e) {
				long end = System.currentTimeMillis();
				if (end - start > 1000) {
					isElementFound = false;
					throw e;
				}
				sleep(1);
			}
		}

		return isElementFound;
	}

	public WebElement findElement(By by, int maxWaitTime) {

		for (WebDriverEventListener l : listeners) {
			SeleniumCustomListener.operation = Operations.GET_TEXT;
			l.beforeFindBy(by, currentElement, driver);
		}

		long start = System.currentTimeMillis();
		while (true) {
			try {
				currentElement = driver.findElement(by);
				break;
			} catch (NoSuchElementException e) {

				long end = System.currentTimeMillis();

				if (end - start > maxWaitTime) {
					throw e;
				}

				sleep(1);
			}
		}

		for (WebDriverEventListener l : listeners) {
			l.afterFindBy(by, currentElement, driver);
		}
		this.by = by;
		return this;
	}

	// WebElement Methods
	@Override
	public void click() {

		for (WebDriverEventListener l : listeners) {
			SeleniumCustomListener.operation = Operations.CLICK;
			l.beforeClickOn(currentElement, driver);
		}

		long start = System.currentTimeMillis();
		while (true) {
			
			try {
				driver.findElement(by).click();
				Thread.sleep(1000);
				break;
			} catch (Exception ex) {
				long end = System.currentTimeMillis();
				if (end - start > 10000) {
					try {
						throw ex;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}

		for (WebDriverEventListener l : listeners) {
			l.afterClickOn(currentElement, driver);
		}

	}

	@Override
	public void submit() {
		driver.findElement(by).submit();
	}

	@Override
	public void sendKeys(CharSequence... keysToSend) {
		for (WebDriverEventListener l : listeners) {
			SeleniumCustomListener.operation = Operations.SEND_KEYS;
			l.beforeChangeValueOf(currentElement, driver, keysToSend);
		}
		
		driver.findElement(by).sendKeys(keysToSend);

		for (WebDriverEventListener l : listeners) {
			l.afterChangeValueOf(currentElement, driver, keysToSend);
		}
	}

	@Override
	public void clear() {
		driver.findElement(by).clear();
	}

	@Override
	public String getTagName() {
		return driver.findElement(by).getTagName();
	}

	@Override
	public String getAttribute(String name) {
		return driver.findElement(by).getAttribute(name);
	}

	@Override
	public boolean isSelected() {
		return driver.findElement(by).isSelected();
	}

	@Override
	public boolean isEnabled() {
		return driver.findElement(by).isEnabled();
	}

	@Override
	public String getText() {
		for (WebDriverEventListener l : listeners) {
			SeleniumCustomListener.operation = Operations.GET_TEXT;
			l.beforeFindBy(by, currentElement, driver);
		}
		int defaultTimeout = 5;
		long start = System.currentTimeMillis();
		String text = null;
		while(true){
			try{
				text = driver.findElement(by).getText();
				if(null ==text || "".equals(text)){
					try {
						Thread.sleep(400);
					} catch (InterruptedException e) {}
					continue;
				}
				break;
			}catch(StaleElementReferenceException|NoSuchElementException se){
				try {
					Thread.sleep(400);
				} catch (InterruptedException e) {}
			}finally {
				long end = System.currentTimeMillis();
				if ((end - start) / 1000 > defaultTimeout) {
					break;
				}
			}
		}
		
		System.out.println("Read text: " + text);
		return text;
	}

	@Override
	public boolean isDisplayed() {
		return driver.findElement(by).isDisplayed();
	}

	@Override
	public Point getLocation() {
		return driver.findElement(by).getLocation();
	}

	@Override
	public Dimension getSize() {
		return driver.findElement(by).getSize();
	}

	@Override
	public Rectangle getRect() {
		return driver.findElement(by).getRect();
	}

	@Override
	public String getCssValue(String propertyName) {
		return driver.findElement(by).getCssValue(propertyName);
	}

	// JavaScript Executor Methods
	//@Override
	public Object executeScript(String script, Object... args) {
		for (WebDriverEventListener l : listeners) {
			l.beforeScript(script, driver);
		}

		Object obj = ((JavascriptExecutor) driver).executeScript(script, args);

		for (WebDriverEventListener l : listeners) {
			l.afterScript(script, driver);
		}

		return obj;
	}

	@Override
	public Object executeAsyncScript(String script, Object... args) {
		return ((JavascriptExecutor) driver).executeAsyncScript(script, args);
	}

	@SuppressWarnings("unused")
	private void waitForTheElement(By by, WebDriver driver, int timeout) {

		long start = System.currentTimeMillis();

		while (true) {

			try {
				driver.findElement(by);
				return;
			} catch (Exception e) {

				try {
					Thread.sleep(500);
				} catch (InterruptedException ex) {
				}

				continue;

			} finally {

				long end = System.currentTimeMillis();

				System.out.println("Timeout: " + (end - start) / 1000);

				if ((end - start) / 1000 > timeout) {
					throw new NoSuchElementException(
							"Timeout exceeded and element couldn't be found");
				}
			}
		}
	}

	@Override
	public void get(String url) {
		driver.get(url);

	}

	@Override
	public String getCurrentUrl() {
		return driver.getCurrentUrl();
	}

	@Override
	public String getTitle() {
		return driver.getTitle();
	}

	@Override
	public String getPageSource() {
		return driver.getPageSource();
	}

	@Override
	public void close() {
		driver.close();
	}

	@Override
	public void quit() {
		driver.quit();
	}

	@Override
	public Set<String> getWindowHandles() {
		return driver.getWindowHandles();
	}

	@Override
	public String getWindowHandle() {
		return driver.getWindowHandle();
	}

	@Override
	public TargetLocator switchTo() {
		sleep(1);
		return driver.switchTo();
	}

	@Override
	public Navigation navigate() {
		return driver.navigate();
	}

	@Override
	public Options manage() {
		return driver.manage();
	}

	private void sleep(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
		}
	}
}
