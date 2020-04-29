package com.practice.bdd.automation.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class TestStubReader {

	private static String stubFileName = null;
	private static Workbook stubWorkbook;
	private static Sheet sheet;
	private static String sheetName;
	private boolean isSheetOpen = false;
	private boolean isSheetClosable= true;
	//WebDriver driver;
    	
	
	public TestStubReader() {
		intiateWorkbook();
		//this.driver = Driver.getInstance();
		
	}

	public void intiateWorkbook() {
		try {
			if(!isSheetOpen){
				stubWorkbook = new XSSFWorkbook(new FileInputStream(new File(stubFileName)));
				isSheetOpen = true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void closeWorkbook() {
		try {
			if(isSheetOpen && isSheetClosable){
				stubWorkbook.close();
				isSheetOpen = false;
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void writeWorkbook() {
		try {
			if(isSheetOpen && isSheetClosable){
				Sheet tempSheet = getSheet();
				stubWorkbook.removeSheetAt(stubWorkbook.getSheetIndex(sheetName));
				Sheet newSheet = stubWorkbook.createSheet(sheetName);
				newSheet = copySheet(tempSheet, newSheet);
				sheet = newSheet;
				stubWorkbook.write(new FileOutputStream(new File(stubFileName)));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private Sheet copySheet(Sheet source, Sheet target){
		for(int i=0;i<source.getPhysicalNumberOfRows();i++){
			Row currRow = source.getRow(i);
			Row newRow = target.createRow(i);
			for(int j=0;j<currRow.getLastCellNum();j++){
				Cell currCell = currRow.getCell(j);
				Cell newCell = newRow.createCell(j);
				if(null != currCell && currCell.getCellTypeEnum() == CellType.STRING){
					System.out.print(currCell.getStringCellValue()+"\t");
					newCell.setCellValue(currCell.getStringCellValue());
				}else{
					System.out.print("Blank\t");
				}
				
			}
			System.out.println();
		}
		return target;
	}
	
	private void printSheet(Sheet sheet){
		for(int i=0;i<sheet.getPhysicalNumberOfRows();i++){
			Row currRow = sheet.getRow(i);
			for(int j=0;j<currRow.getLastCellNum();j++){
				Cell currCell = currRow.getCell(j);
				if(null != currCell && currCell.getCellTypeEnum() == CellType.STRING){
					System.out.print(currCell.getStringCellValue()+"\t");
				}else{
					System.out.print("Blank\t");
				}
				
			}
			System.out.println();
		}
	}
	
	private void printSheet(){
		for(int i=0;i<sheet.getPhysicalNumberOfRows();i++){
			Row currRow = sheet.getRow(i);
			for(int j=0;j<currRow.getLastCellNum();j++){
				Cell currCell = currRow.getCell(j);
				if(null != currCell && currCell.getCellTypeEnum() == CellType.STRING){
					System.out.print(currCell.getStringCellValue()+"\t");
				}else{
					System.out.print("Blank\t");
				}
				
			}
			System.out.println();
		}
	}
	private void printWorkbook(){
		for(int k = 0;k<stubWorkbook.getNumberOfSheets();k++){
			Sheet sheet = stubWorkbook.getSheetAt(k);
			System.out.println("Sheet name is "+sheet.getSheetName());
			for(int i=0;i<sheet.getPhysicalNumberOfRows();i++){
				Row currRow = sheet.getRow(i);
				for(int j=0;j<currRow.getLastCellNum();j++){
					Cell currCell = currRow.getCell(j);
					if(null != currCell && currCell.getCellTypeEnum() == CellType.STRING){
						System.out.print(currCell.getStringCellValue()+"\t");
					}else{
						System.out.print("Blank\t");
					}
					
				}
				System.out.println();
			}
		}
		
	}
	
	public Sheet getSheet(){
		if(sheet == null){
			if(null == sheetName){
				System.out.println("Sheet name has not been set or not existing");
			}else{
				sheet = stubWorkbook.getSheet(sheetName);
			}
		}
		return sheet;
	}

	public void addSheetIfNotExisting(String sheetName) {
		TestStubReader.sheetName = sheetName;
		intiateWorkbook();
		for (int i = 0; i < stubWorkbook.getNumberOfSheets(); i++) {
			Sheet s = stubWorkbook.getSheetAt(i);
			if (s.getSheetName().equalsIgnoreCase(sheetName)) {
				return;
			}
		}
		stubWorkbook.createSheet(sheetName);
		TestStubReader.sheetName = sheetName;
		TestStubReader.sheet = stubWorkbook.getSheet(sheetName);
		writeWorkbook();
		closeWorkbook();
		clearSheet(sheet);
		closeWorkbook();
	}

	public void clearSheet() {
		intiateWorkbook();
		sheet = getSheet();
		for (int rowIndex = sheet.getPhysicalNumberOfRows() - 1; rowIndex >= 0; rowIndex--) {
			sheet.removeRow(sheet.getRow(rowIndex));
		}
		writeWorkbook();
		closeWorkbook();
	}
	
	public void clearSheet(Sheet sheet) {
		intiateWorkbook();
		sheet = getSheet();
		for (int rowIndex = sheet.getPhysicalNumberOfRows() - 1; rowIndex >= 0; rowIndex--) {
			sheet.removeRow(sheet.getRow(rowIndex));
		}
		writeWorkbook();
		closeWorkbook();
	}

	public void addData(String header, String value) {
		addData(header, value, true);
	}

	public void addData(String header, String value, boolean append) {
		intiateWorkbook();
		boolean isHeaderExisting = false;
		sheet = getSheet();
		for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {
			Row currRow = sheet.getRow(i);
			String rowHeader = currRow.getCell(0).getStringCellValue();
			if (rowHeader.equals(header)) {
				isHeaderExisting = true;
				int cellIndex = 1;
				while (null != currRow.getCell(cellIndex)) {
					cellIndex++;
				}
				if (append) {
					currRow.createCell(cellIndex).setCellValue(value);
				}else{
					if(1 != cellIndex)
						currRow.createCell(--cellIndex).setCellValue(value);
					else
						currRow.createCell(cellIndex).setCellValue(value);
				}
				break;
			}
		}
		if (!isHeaderExisting) {
			Row lastRow = sheet.createRow(sheet.getPhysicalNumberOfRows());
			lastRow.createCell(0).setCellValue(header);
			lastRow.createCell(1).setCellValue(value);
		}
		writeWorkbook();
		closeWorkbook();
	}
		
	public void addData(String header, String value, int pos) {
		intiateWorkbook();
		boolean isHeaderExisting = false;
		sheet = getSheet();
		for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {
			Row currRow = sheet.getRow(i);
			String rowHeader = currRow.getCell(0).getStringCellValue();
			if (rowHeader.equals(header)) {
				isHeaderExisting = true;
				int cellIndex = 1;
				
				if(null != currRow.getCell(pos)){
					currRow.createCell(pos).setCellValue(value);
				}else{
					while (null != currRow.getCell(cellIndex)) {
						cellIndex++;
					}
					while(cellIndex <pos){
						currRow.createCell(cellIndex++);
					}
					currRow.createCell(pos).setCellValue(value);
				}
				
				break;
			}
		}
		if (!isHeaderExisting) {
			Row lastRow = sheet.createRow(sheet.getPhysicalNumberOfRows());
			lastRow.createCell(0).setCellValue(header);
			lastRow.createCell(1).setCellValue(value);
		}
		writeWorkbook();
		closeWorkbook();
	}

	public boolean setReadingSheet(String sheetName){
		intiateWorkbook();
		boolean isSheetExisting = false;
		for(int i=0;i<stubWorkbook.getNumberOfSheets();i++){
			Sheet s = stubWorkbook.getSheetAt(i);
			if(s.getSheetName().equalsIgnoreCase(sheetName)){
				isSheetExisting = true;
				TestStubReader.sheetName = sheetName;
				sheet = null;
				break;
			}
		}
		closeWorkbook();
		if(!isSheetExisting){
			TestStubReader.sheetName = null;
			
		}
		return isSheetExisting;
	}

	public String getData(String header, int testDataNum) {
		intiateWorkbook();
		String data = null;
		sheet = getSheet();
		for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {
			Row currRow = sheet.getRow(i);
			String rowHeader = currRow.getCell(0).getStringCellValue();
			if (rowHeader.equals(header)) {
				return extractCellData(currRow.getCell(testDataNum));
				
			}
		}
		closeWorkbook();
		return data;
	}
	
	
	private String extractCellData(Cell cell){
		String cellContent=null;
		cell.setCellType(CellType.STRING);
		return cell.getStringCellValue();
		/*switch(cell.getCellTypeEnum()){
		case STRING:
			cellContent = cell.getStringCellValue();
			break;
		case BOOLEAN:
			cellContent = String.valueOf(cell.getBooleanCellValue());
			break;
		case NUMERIC:
			double d = cell.getNumericCellValue();
			long ll = (long)d;
			if(d == ll){
				cellContent = String.valueOf(d);
			}else{
				cellContent = String.valueOf(ll);
			}
			break;
		default:
			System.out.println("Invalid data found in "+cell.getAddress());
		}
		return cellContent;*/
	}
	
	public List<String> getAllValuesForHeader(String header) {
		intiateWorkbook();
		List<String> allData = new ArrayList<String>();
		sheet = getSheet();
		for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {
			Row currRow = sheet.getRow(i);
			String rowHeader = currRow.getCell(0).getStringCellValue();
			allData.add(rowHeader);
			if (rowHeader.equals(header)) {
				for(int j=0;j<currRow.getLastCellNum();j++){
					Cell currCell = currRow.getCell(j);
					if(null != currCell && currCell.getCellTypeEnum() == CellType.STRING){
						allData.add(currCell.getStringCellValue());
					}
				}
			}
		}
		closeWorkbook();
		return allData;
	}

	public boolean checkForPresenceOfValueForHeader(String header, String value) {
		intiateWorkbook();
		boolean isValuePresent = false;
		sheet = getSheet();
		try {
			for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {
				Row currRow = sheet.getRow(i);
				String rowHeader = currRow.getCell(0).getStringCellValue();
				if (rowHeader.equals(header)) {
					for (int j = 1; j < currRow.getLastCellNum(); j++) {
						String valueForHeader = currRow.getCell(j).getStringCellValue();
						if (valueForHeader.equals(value)) {
							isValuePresent = true;
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		closeWorkbook();
		return isValuePresent;
	}
	public String getValueAgainstAnotherValue(String primeHeader, String primeValue, String targetHeader){
		intiateWorkbook();
		String targetValue=null;
		sheet = getSheet();
		int targetValueIndex=-1;
		try {
			for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {
				Row currRow = sheet.getRow(i);
				String rowHeader = currRow.getCell(0).getStringCellValue();
				if (rowHeader.equals(primeHeader)) {
					for (int j = 1; j < currRow.getLastCellNum(); j++) {
						String valueForHeader = currRow.getCell(j).getStringCellValue();
						if (valueForHeader.equals(primeValue)) {
							targetValueIndex = j;
							break;
						}
					}
				}
			}
			for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {
				Row currRow = sheet.getRow(i);
				String rowHeader = currRow.getCell(0).getStringCellValue();
				if (rowHeader.equals(targetHeader)) {
					if(targetValueIndex == -1){
						return null;
					}
					Cell targetValueCell = currRow.getCell(targetValueIndex);
					if(null != targetValueCell){
						targetValue = targetValueCell.getStringCellValue();
						break;
					}
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		closeWorkbook();
		return targetValue;
	}
	

	
	private int getValueIndexForHeader(String header, String value){
		int index = -1;
		for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {
			Row currRow = sheet.getRow(i);
			String rowHeader = currRow.getCell(0).getStringCellValue();
			if (rowHeader.equals(header)) {
				for (int j = 1; j < currRow.getLastCellNum(); j++) {
					String valueForHeader = currRow.getCell(j).getStringCellValue();
					if (valueForHeader.equals(value)) {
						index = j;
					}
				}
			}
		}
		
		return index;
	}
	
	public String setValueAgainstAnotherValue(String primeHeader, String primeValue, String targetHeader, String targetValue){
		intiateWorkbook();
		sheet = getSheet();
		int targetValueIndex=0;
		boolean isPrimeValueFound=false;
		boolean isTargetHeaderFound = false;
		try {
			for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {
				Row currRow = sheet.getRow(i);
				String rowHeader = currRow.getCell(0).getStringCellValue();
				if (rowHeader.equals(primeHeader)) {
					for (int j = 1; j < currRow.getLastCellNum(); j++) {
						String valueForHeader = currRow.getCell(j).getStringCellValue();
						if (valueForHeader.equals(primeValue)) {
							targetValueIndex = j;
							isPrimeValueFound = true;
						}
					}
				}
			}
			if(!isPrimeValueFound){
				isSheetClosable = false;
				addData(primeHeader, primeValue, true);
				isSheetClosable = true;
				targetValueIndex = getValueIndexForHeader(primeHeader, primeValue);
			}
			for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {
				Row currRow = sheet.getRow(i);
				String rowHeader = currRow.getCell(0).getStringCellValue();
				if (rowHeader.equals(targetHeader)) {
					isTargetHeaderFound = true;
					Cell targetValueCell = currRow.getCell(targetValueIndex);
					if(null != targetValueCell){
						targetValue = targetValueCell.getStringCellValue();
						break;
					}else{
						Cell newTargetCell=null;
						for(int j=1;j<=targetValueIndex;j++){
							newTargetCell = currRow.getCell(j);
							if(null == newTargetCell)
								newTargetCell = currRow.createCell(j);
						}
						newTargetCell.setCellValue(targetValue);
					}
					break;
				}
			}
			
			
			if(!isTargetHeaderFound){
				Row lastRow = sheet.createRow(sheet.getPhysicalNumberOfRows());
				lastRow.createCell(0).setCellValue(targetHeader);
				Cell newTargetCell=null;
				for(int j=1;j<=targetValueIndex;j++){
					newTargetCell = lastRow.createCell(j);
				}
				newTargetCell.setCellValue(targetValue);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		writeWorkbook();
		closeWorkbook();
		return targetValue;
	}

	public String setSheetAndGetData(String sheetName, String header, int colIndex){
        
        setReadingSheet(sheetName);
        return getData(header,colIndex);
 }


	//Sushant //  read data using column name
	
		public String getData(String Rowheader, String ColHeader ) {
			intiateWorkbook();
			String data = null;
			sheet = getSheet();	
			int colDataNum = getValueIndexForHeaderCell(ColHeader);
			for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {
				Row currRow = sheet.getRow(i);
				String rowHeader = currRow.getCell(0).getStringCellValue();
				if (rowHeader.equals(Rowheader)) {
					return extractCellData(currRow.getCell(colDataNum));
				}
			}
			closeWorkbook();
			return data;
		}
		
		
		public int getValueIndexForHeaderCell(String HeaderCellvalue){
			int index = -1;
				Row currRow = sheet.getRow(0);		
				//String rowHeader = currRow.getCell(0).getStringCellValue();
					for (int j = 1; j < currRow.getLastCellNum(); j++) {
						String valueForHeader = currRow.getCell(j).getStringCellValue();
						if (valueForHeader.equals(HeaderCellvalue)) {
							index = j;	
						}
					}
			return index;
		}

	
		
		public String getFirstCellData() {
			intiateWorkbook();
			String data = null;
			sheet = getSheet();
				Row currRow = sheet.getRow(0);
				String rowHeader = currRow.getCell(0).getStringCellValue();
				
			closeWorkbook();
			return rowHeader;
		}
		
		public void setData(String sheetName, String header, String value, int colIndex){
			
			setReadingSheet(sheetName);
	         addData(header,value,colIndex);
		}
		
		
		
		
		
	
	
}
