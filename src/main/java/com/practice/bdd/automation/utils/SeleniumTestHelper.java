package com.practice.bdd.automation.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerFactory;

import org.apache.commons.io.FileUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebDriverException;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.Select;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;

public class SeleniumTestHelper {

	static WebDriver driver = Driver.getInstance();

	public enum DropDownMode {
		VISIBLE_TEXT, INDEX, VALUE
	}

	public static void selectFromDropDown(WebElement element, String value, DropDownMode mode) {
		try {
			Select select = new Select(element);

			switch (mode) {
			case VISIBLE_TEXT:
				select.selectByVisibleText(value);
				break;
			case INDEX:
				select.selectByIndex(Integer.parseInt(value));
				break;
			case VALUE:
				select.selectByValue(value);

			}
			retryCount = 0;

		} catch (Exception exe) {

			if (retryCount > 3) {
				throw exe;
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			retryCount++;
			selectFromDropDown(element, value, mode);

		}
	}

	private static int retryCount = 0;

	public static boolean isElementDisplayed(WebElement element) {
		boolean displayed = false;
		try {
			if (element.isDisplayed()) {
				displayed = true;
			}
		} catch (NoSuchElementException exeNo) {
			displayed = false;
		}
		return displayed;
	}

	public static WebElement WaitForElement(WebElement element, int seconds) {

		long start = System.currentTimeMillis();

		while (true) {

			try {
				element.isDisplayed();
				return element;
			} catch (WebDriverException e) {

				try {
					Thread.sleep(500);
				} catch (InterruptedException ex) {
				}

				continue;

			} finally {

				long end = System.currentTimeMillis();
				if ((end - start) / 1000 > seconds) {
					throw new NoSuchElementException("Timeout exceeded and element couldn't be found");
				}
			}
		}
	}

	public static void click(WebElement element) {

		try {
			element.click();
		} catch (WebDriverException e) {

			if (e instanceof NoSuchElementException) {
				WaitForElement(element, 10);
				element.click();
				return;
			} else if (e instanceof StaleElementReferenceException) {
				Driver.getInstance().navigate().refresh();
				element.click();
				return;
			}

			if (e.getMessage().startsWith("unknown error: Element")) {
				((JavascriptExecutor) (Driver.getInstance())).executeScript("arguments[0].click()", element);
				return;
			}

			throw e;
		}
	}

	public static void waitForElementToBeDisplayed(WebDriver driver, WebElement element, int timeOutInSeconds) {
		WebDriverWait wait = new WebDriverWait(driver, timeOutInSeconds);
		wait.until(ExpectedConditions.visibilityOf(element));
	}

	public static void waitForElementToBeClickable(WebDriver driver, WebElement element, int timeOutInSeconds) {
		WebDriverWait wait = new WebDriverWait(driver, timeOutInSeconds);
		wait.until(ExpectedConditions.elementToBeClickable(element));
	}

	public static void waitForElementToBePresent(WebDriver driver, By element, int timeOutInSeconds) {
		WebDriverWait wait = new WebDriverWait(driver, timeOutInSeconds);
		wait.until(ExpectedConditions.presenceOfElementLocated(element));
	}

	public static void scrollToElement(WebDriver driver, WebElement element) {
		Actions actions = new Actions(driver);
		actions.moveToElement(element);
		actions.perform();
	}

	public static void switchingToParentWindow(WebDriver driver) {

		Set<String> childWindow = driver.getWindowHandles();
		Iterator<String> itrs = childWindow.iterator();
		String pWindows = itrs.next();
		String cWindows = itrs.next();
		driver.switchTo().window(cWindows);

		driver.switchTo().window(pWindows);

	}

	public static boolean verifyElement(By locator) {
		boolean isFound = true;
		WebDriverDispatcher wpatch = (WebDriverDispatcher) Driver.getInstance();
		try {
			isFound = wpatch.verifyElement(locator, 20);
		} catch (Exception e) {
			isFound = false;
		}
		return isFound;
	}

	public static boolean verifyElement(By locator, int maxWaitTime) {
		boolean isFound = true;
		WebDriverDispatcher wpatch = (WebDriverDispatcher) Driver.getInstance();
		try {
			isFound = wpatch.verifyElement(locator, maxWaitTime);
		} catch (Exception e) {
			isFound = false;
		}
		return isFound;
	}

	public static WebElement locateRow(String description) {
		return Driver.getInstance().findElement(By.partialLinkText(description));
	}

	public static WebDriver switchToOtherWindow(WebDriver driver) {
		String currentHandle = driver.getWindowHandle();
		Set<String> handles = driver.getWindowHandles();
		for (String handle : handles) {
			if (!handle.equals(currentHandle)) {
				driver.switchTo().window(handle);
			}
		}
		return driver;
	}

	public static WebDriver switchToOtherWindowAndCloseCurrentWindow(WebDriver driver) {
		String currentHandle = driver.getWindowHandle();
		Set<String> handles = driver.getWindowHandles();
		for (String handle : handles) {
			if (!handle.equals(currentHandle)) {
				driver.close();
				driver.switchTo().window(handle);
			}
		}
		return driver;
	}

	public static WebDriver switchToOtherWindowAndCloseItAndComeBack(WebDriver driver) {
		String currentHandle = driver.getWindowHandle();
		Set<String> handles = driver.getWindowHandles();
		for (String handle : handles) {
			if (!handle.equals(currentHandle)) {
				driver.switchTo().window(handle);
				driver.close();
				break;
			}
		}
		driver.switchTo().window(currentHandle);
		return driver;
	}

	public static void switchingTochildtWindow(WebDriver driver) {

		Set<String> childWindow = driver.getWindowHandles();
		Iterator<String> itrs = childWindow.iterator();
		String pWindows = itrs.next();
		String cWindows = itrs.next();
		driver.switchTo().window(cWindows);

	}

	public static void waitForElementToAppear(By element, long minWaitTime, long maxWaitTime) {
		boolean isElementFound = false;
		try {
			Thread.sleep(minWaitTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long maxDeltaTime = maxWaitTime - minWaitTime;
		long startTime = System.currentTimeMillis();
		long currentTime = startTime;

		while ((maxDeltaTime > (currentTime - startTime)) && !isElementFound) {
			isElementFound = true;
			try {
				Driver.getInstance().findElement(element);
			} catch (Exception e) {
				try {
					Thread.sleep(500);
					Driver.getInstance().navigate().refresh();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				isElementFound = false;
			}
			currentTime = System.currentTimeMillis();
		}
	}

	public static String getScreenshot() {
		String dateName = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
		WebDriver driver = Driver.getInstance();
		TakesScreenshot ts = (TakesScreenshot) driver;
		File source = ts.getScreenshotAs(OutputType.FILE);
		String destination = System.getProperty("user.dir") + "/FailedTestsScreenshots/" + dateName + ".png";
		File finalDestination = new File(destination);
		try {
			FileUtils.copyFile(source, finalDestination);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return destination;
	}

	public static int generateRandomInt(int min, int max) {
		Random foo = new Random();
		return foo.nextInt((max + 1) - min) + min;
	}

	public static void jsClick(WebElement element) {
		JavascriptExecutor js = (JavascriptExecutor) Driver.getInstance();
		js.executeScript("arguments[0].click();", element);
	}

	/**
	 * 
	 * @author nurmamet.hemrayev
	 * @param element
	 */
	public static void actionsClick(WebElement element) {
		Actions actions = new Actions(Driver.getInstance());
		actions.moveToElement(element).perform();
		actions.click().perform();
	}

	/**
	 * This method is used for selecting date from calendar modal dialog
	 * 
	 * @author nurmamet.hemrayev
	 * @param days
	 *            - required days from current date
	 * @param months
	 *            - required months from current date
	 * @param years
	 *            - required years from current date
	 */
	public static void setCalendarFromCurrentDate(int days, int months, int years) {
		Calendar cal = Calendar.getInstance();
		if (0 != days) {
			cal.add(Calendar.DATE, days);
		}
		if (0 != months) {
			cal.add(Calendar.MONTH, months);
		}
		if (0 != years) {
			cal.add(Calendar.YEAR, years);
		}

		Date reqDate = cal.getTime();
		SimpleDateFormat fullDateFormat = new SimpleDateFormat("yyyy-MMMM-dd");
		SimpleDateFormat shortMonthFormat = new SimpleDateFormat("MMM");

		String reqDateStr = fullDateFormat.format(reqDate);
		String shortMonth = shortMonthFormat.format(reqDate);
		String reqYear = reqDateStr.split("-")[0];
		String reqMonth = reqDateStr.split("-")[1];
		String reqDay = reqDateStr.split("-")[2];
		reqDay = reqDay.replaceFirst("^0+(?!$)", "");
		WebDriver driver = Driver.getInstance();
		By dispYearMonthBy = By.xpath("//div[@class='datepicker-days']//th[@class='datepicker-switch']");
		By currentYearBy = By.xpath("//div[@class='datepicker-months']//th[@class='datepicker-switch']");
		By yearRangeBy = By.xpath("//div[@class='datepicker-years']//th[@class='datepicker-switch']");
		String dispYearMonth = driver.findElement(dispYearMonthBy).getText();
		String dispMonth = dispYearMonth.split(" ")[0];
		String dispYear = dispYearMonth.split(" ")[1];
		By nextBtnBy = By.xpath("//th[@class='next'][not(./ancestor::*[contains(@style,'display: none')])]");
		if (!dispYear.equals(reqYear)) {
			driver.findElement(dispYearMonthBy).click();
			driver.findElement(currentYearBy).click();
			String currentyearRange = driver.findElement(yearRangeBy).getText();
			int startYear = Integer.parseInt(currentyearRange.split("-")[0]);
			int endYear = Integer.parseInt(currentyearRange.split("-")[1]);
			int reqYearInt = Integer.parseInt(reqYear);
			while (!(reqYearInt >= startYear && reqYearInt <= endYear)) {
				driver.findElement(nextBtnBy).click();
				currentyearRange = driver.findElement(yearRangeBy).getText();
				startYear = Integer.parseInt(currentyearRange.split("-")[0]);
				endYear = Integer.parseInt(currentyearRange.split("-")[1]);
			}
			driver.findElement(By.xpath("//span[contains(@class,'year') and text()='" + reqYear + "']")).click();
			;
			driver.findElement(By.xpath("//span[contains(@class,'month') and text()='" + shortMonth + "']")).click();
		}
		dispYearMonth = driver.findElement(dispYearMonthBy).getText();
		dispMonth = dispYearMonth.split(" ")[0];

		if (!(dispMonth.equals(reqMonth))) {
			driver.findElement(dispYearMonthBy).click();
			driver.findElement(By.xpath("//span[contains(@class,'month') and text()='" + shortMonth + "']")).click();
		}

		driver.findElement(
				By.xpath("//div[@class='datepicker-days']//tbody//td[@class='day' and text()='" + reqDay + "']"))
				.click();
	}

	public static String getAbsolutePath(String fileName) {

		String relativePath = Config.getProperty("attachmentsPath") + Config.getProperty(fileName);
		Path path = Paths.get(relativePath);
		Path absolutePath = path.toAbsolutePath();

		return absolutePath.toString();
	}

	public static File getLastDownloadedFile() {
		try {
			Thread.sleep(7000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		File requiredFile = null;
		String requiredFileName = "";
		long lastModified = 0;
		String downloadDir = Config.getDownloadLocation();
		do {
			File f = new File(downloadDir);
			if (f.isDirectory())
				for (File currFile : f.listFiles()) {
					if (!currFile.isDirectory()) {
						if (lastModified < currFile.lastModified()) {
							requiredFile = currFile;
							lastModified = currFile.lastModified();
						}
					}
				}
			requiredFileName = requiredFile.getName();
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// lastModified = 0;
		} while (requiredFileName.contains(".crdownload") || requiredFileName.contains(".download"));
		return requiredFile;
	}

	public static File getLastDownloadedFile(String matchingFileName) {
		try {
			Thread.sleep(7000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		File requiredFile = null;
		String requiredFileName = "";
		long lastModified = 0;
		String downloadDir = Config.getDownloadLocation();
		do {
			File f = new File(downloadDir);
			if (f.isDirectory())
				for (File currFile : f.listFiles()) {
					if (!currFile.isDirectory()) {
						if (currFile.getName().contains(matchingFileName) && lastModified < currFile.lastModified()) {
							requiredFile = currFile;
							lastModified = currFile.lastModified();
						}
					}
				}
			requiredFileName = requiredFile.getName();
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// lastModified = 0;
		} while (requiredFileName.contains(".crdownload") || requiredFileName.contains(".download"));
		return requiredFile;
	}

	public static void waitForTextToBePresentInElement(WebDriver driver, WebElement element, int timeOutInSeconds,
			String text) {
		WebDriverWait wait = new WebDriverWait(driver, timeOutInSeconds);
		wait.until(ExpectedConditions.textToBePresentInElement(element, text));
	}

	public static void switchingTochildtWindowAndCloseIt(WebDriver driver) {

		Set<String> childWindow = driver.getWindowHandles();
		Iterator<String> itrs = childWindow.iterator();
		String pWindows = itrs.next();
		String cWindows = itrs.next();
		driver.switchTo().window(cWindows);
		driver.switchTo().window(cWindows).close();

	}

	public static void uploadfile(String filename) throws IOException {
		String script = System.getProperty("user.dir") + "\\" + "upload.exe";
		// Runtime.getRuntime().exec(script+"
		// "+System.getProperty("user.dir")+"\\"+filename);
		Runtime.getRuntime().exec(script + " " + filename);
		System.out.println(System.getProperty("user.dir") + "\\" + filename);
	}

	public static String convertxmltostring(String filename) throws IOException {
		// filename is filepath string
		BufferedReader br = new BufferedReader(new FileReader(new File(filename)));
		String line;
		StringBuilder sb = new StringBuilder();

		while ((line = br.readLine()) != null) {
			sb.append(line.trim());
		}

		return sb.substring(3);
	}

	public static String XMLfile_update_singleinstance(String ASNID, String Parenttag, String Childtag, String filename)
			throws SAXException, IOException, ParserConfigurationException {
		String filePath = "D:\\Request.xml";
		File xmlFile = new File(filePath);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		try {
			dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(xmlFile);
			doc.getDocumentElement().normalize();
			updateElementValue_generic(doc, Parenttag, Childtag, ASNID);
			doc.getDocumentElement().normalize();

			// write the updated document to file or console
			doc.getDocumentElement().normalize();
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(doc);
			StreamResult result = new StreamResult(new File(filename));
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.transform(source, result);
			System.out.println("XML file updated successfully");

		} catch (SAXException | ParserConfigurationException | IOException | TransformerException e1) {
			e1.printStackTrace();
		}
		return ASNID;
	}

	private static void updateElementValue_generic(Document doc, String Parentnode, String Childnode,
			String childvalue) {
		NodeList Parentnodes = doc.getElementsByTagName(Parentnode);
		Element child = null;
		// loop for each Parentnodes
		for (int i = 0; i < Parentnodes.getLength(); i++) {
			child = (Element) Parentnodes.item(i);
			Node name = child.getElementsByTagName(Childnode).item(0).getFirstChild();
			name.setNodeValue(childvalue);
		}
	}

	public static void Close_OpenedWindow(String Screenname, WebDriver driver) throws InterruptedException {
		String Screenclose = "(//label[text()='" + Screenname + "']//following::span[text()='Close'])[1]";
		driver.findElement(By.xpath(Screenclose)).click();

	}

	public static void doubleclickonelement(WebDriver driver, WebElement ele) {
		Actions action = new Actions(driver);
		action.moveToElement(ele).doubleClick(ele).build().perform();
	}

	public static String Getvalueinbetweentags(String tagname, String Response) {
		final Pattern TAG_REGEX = Pattern.compile("<" + tagname + ">(.+?)</" + tagname + ">", Pattern.DOTALL);
		final Matcher matcher = TAG_REGEX.matcher(Response);
		matcher.find();
		System.out.println(matcher.group(1));
		if (matcher.group(1).equalsIgnoreCase("Confirmation")) {
			System.out.println("Response success");
		}
		return matcher.group(1);

	}

	public static void switchToInnerFrame(WebDriver driver) {
		try {
			Thread.sleep(4000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		driver.switchTo().defaultContent();
		driver.switchTo().frame(0);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
