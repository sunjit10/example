package com.sap.conn.jco.examples.client.beginner;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoField;
import com.sap.conn.jco.JCoFieldIterator;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoMetaData;
import com.sap.conn.jco.JCoParameterList;
import com.sap.conn.jco.JCoStructure;
import com.sap.conn.jco.JCoTable;

/*
 * Usage: GetFunctionModuleMetrics <FunctionName> 
 * Ex:
 * GetFunctionModuleMetrics SWNC_GET_WORKLOAD_SNAPSHOT
 * GetFunctionModuleMetrics SWNC_GET_WORKLOAD_STATRECS 
 * GetFunctionModuleMetrics SAPWL_SNAPSHOT_FROM_REMOTE_SYS
 * GetFunctionModuleMetrics TH_SERVER_LIST
 * GetFunctionModuleMetrics TH_USER_LIST
 * GetFunctionModuleMetrics /SDF/GET_DUMP_LOG
 * 
 */
public class GetFunctionModuleMetrics {
	
	private static boolean DEBUG = false;
	private static Gson gson = new GsonBuilder().setPrettyPrinting().create();
	private static ObjectMapper mapper = new ObjectMapper(); 
	private static String functionName;
	
	private static File dir = null;
	
	// Date format is yyyyMMdd
	private static String startDateStr = "20230803";
	private static String endDateStr = "20230803";
	private static String startTimeStr = "02:55:00";
	private static String endTimeStr = "23:30:00";
	
	private static StringBuffer output = new StringBuffer();
	
	private static JCoFunction function;
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 1) {
			throw new Exception("Usage: GetFunctionModuleMetrics <FunctionName>");
		}
		
		mapper.disable(JsonWriteFeature.QUOTE_FIELD_NAMES.mappedFeature());
	    mapper.enable(SerializationFeature.INDENT_OUTPUT);
	    
		
		functionName = args[0];
	
		JCoDestination destination = 
				JCoDestinationManager.getDestination(DestinationConcept.SomeSampleDestinations.ABAP_AS1);
		
		function = 
				destination.getRepository().getFunction(functionName);

		if (function == null) {
			throw new RuntimeException(functionName +  " not found in SAP.");
		}

		//function.getImportParameterList().setValue("READ_USERNAME", "SPERF");

		DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		Date startDate = formatter.parse(startDateStr);
		Date endDate = formatter.parse(endDateStr);
		
		// Function Modules that don't have any Import Parameters
		String[] noImportParameterModules = {
				"TH_USER_LIST",
				"TH_WPINFO",
				"Z_FUNC_CALL_TFDIR",
				"ICM_GET_INFO",
				"ICM_GET_INFO2",
				"ICM_RELINFO",
				"ICM_MPI_INFO",
				"GET_CLIENT_REMOTE",
				"GET_CLIENT_REMOTE_3X",
				"ICM_PARAM_INFO"
		};
		
		
		if (Arrays.asList(noImportParameterModules).contains(functionName)) {
				// No Import Parameter List
		} else if (functionName.equals("/SDF/GET_DUMP_LOG")) {
			function.getImportParameterList().setValue("DATE_FROM", startDate);
			function.getImportParameterList().setValue("DATE_TO", endDate);
			functionName = "GET_DUMP_LOG";
			//function.getImportParameterList().setValue("TIME_FROM", startTimeStr);
			//function.getImportParameterList().setValue("TIME_TO", endTimeStr);
		} else if (functionName.equals("/SDF/EWA_GET_ABAP_DUMPS")) {
			// No Import Parameter List
			functionName = "EWA_GET_ABAP_DUMPS";
		} else if (functionName.equals("RFC_READ_TABLE")) {
			//function.getImportParameterList().setValue("QUERY_TABLE", "USR41");
			//function.getImportParameterList().setValue("QUERY_TABLE", "TFDIR");
			String tableToQuery = "TBTCO";
			//String tableToQuery = "DBCON";
			function.getImportParameterList().setValue("QUERY_TABLE", tableToQuery);
			
			JCoParameterList tableParameterList = function.getTableParameterList();
			if (tableParameterList != null) {
				JCoFieldIterator iterator = tableParameterList.getFieldIterator();
				while (iterator.hasNextField()) {
					JCoField field = iterator.nextField();
					//String fieldType = field.getTypeAsString();
					
					if (tableToQuery.equals("TBTCO") && field.getName().equals("FIELDS")) {
						JCoTable table = field.getTable();
						table.appendRows(2);
						table.setRow(0);
						table.setValue("FIELDNAME", "JOBNAME");
						table.setRow(1);
						table.setValue("FIELDNAME", "JOBCOUNT");
						/*
						 * table.setRow(2); table.setValue("FIELDNAME", "STATUS");
						 */
					}
					
					if (tableToQuery.equals("DBCON") && field.getName().equals("FIELDS")) {
						JCoTable table = field.getTable();
						table.appendRows(1);
						table.setRow(0);
						
						  //table.setValue("FIELDNAME", "CON_NAME"); table.setRow(1);
						 
						//table.setValue("FIELDNAME", "DBMS");
						
						  //table.setRow(2); table.setValue("FIELDNAME", "USER_NAME");
						  table.setRow(2); table.setValue("FIELDNAME", "DB_RECO");
						 
						/*
						 * table.setRow(3); table.setValue("FIELDNAME", "CON_ENV");
						 */
						/*
						 * table.setRow(3); table.setValue("FIELDNAME", "DB_RECO"); table.setRow(4);
						 * table.setValue("FIELDNAME", "MAX_CONNECTIONS"); table.setRow(5);
						 * table.setValue("FIELDNAME", "OPT_CONNECTIONS");
						 */
						
					}
				}
			}
		} else if (functionName.equals("/SDF/GET_JOB_INFO")) {
			JCoParameterList importParameterList = function.getImportParameterList();
			
			if (importParameterList != null) {
				JCoFieldIterator iterator = importParameterList.getFieldIterator();
				while (iterator.hasNextField()) {
					JCoField field = iterator.nextField();
					String fieldType = field.getTypeAsString();
					if (field.getName().equals("IT_JOB_HEAD") && fieldType.equals("TABLE")) {
						JCoTable table = field.getTable();
						table.appendRows(1);
						table.setRow(0);
						// You can get the JOBNAME and JOBCOUNT from RFC_READ_TABLE TBTCO
						table.setValue("JOBNAME", "ZEMPJOB2");
						table.setValue("JOBCOUNT", "18323200");
					}
				}
			}
			functionName = "GET_JOB_INFO";
		}  else if (functionName.equals("TH_SERVER_LIST")) {
			function.getImportParameterList().setValue("SERVICES",  Byte.valueOf("25").byteValue());
			function.getImportParameterList().setValue("SYSSERVICE",  Byte.valueOf("00").byteValue());
			function.getImportParameterList().setValue("ACTIVE_SERVER", 1);
			function.getImportParameterList().setValue("SUBSYSTEM_AWARE", 1);
		} else {
			
			// SWNC_GET_WORKLOAD_SNAPSHOT
			// SWNC_GET_WORKLOAD_STATRECS
			function.getImportParameterList().setValue("READ_START_DATE", startDate);
			function.getImportParameterList().setValue("READ_END_DATE", endDate);

			function.getImportParameterList().setValue("READ_START_TIME", startTimeStr);
			function.getImportParameterList().setValue("READ_END_TIME", endTimeStr);
			
			// ************ Checking for a specific Client ID
			//function.getImportParameterList().setValue("READ_CLIENT", "300");
		}

		try {
			function.execute(destination);
		} catch (Exception e) {
			System.out.println(e);
			return;
		}
		
		dir = new File(functionName);
		if (!dir.exists()) {
			dir.mkdir();
		}
		
		fetchMetaData();
		fetchMetrics();
	}
	
	private static void fetchMetaData() {
		JCoParameterList parameterList = function.getExportParameterList();
		if (parameterList != null) {
			JCoFieldIterator iterator = parameterList.getFieldIterator();
			while (iterator.hasNextField()) {
				JCoField field = iterator.nextField();
				String fieldType = field.getTypeAsString();
				output.append("------------------ " + field.getName() + " -------------------------- \n");

				if (fieldType!= null && fieldType.equals("TABLE")) {
					JCoTable table = field.getTable();
					JCoMetaData metaData = table.getMetaData();
					String actualTableName = metaData.getName();
					printMetaData(actualTableName, table, metaData);
				} else if (fieldType != null && fieldType.equals("STRUCTURE")) {
					JCoStructure val = field.getStructure();
					printStructure("", val);
				} 
				output.append("\n");
			}
		}
		
		
		JCoParameterList tableParameterList = function.getTableParameterList();
		if (tableParameterList != null) {
			if (DEBUG) {
				// System.out.println("Displaying Export ParameterList");
				// System.out.println("Field Count " + parameterList.getFieldCount());
			}
			
			JCoFieldIterator iterator = tableParameterList.getFieldIterator();
			while (iterator.hasNextField()) {
				JCoField field = iterator.nextField();
				String fieldType = field.getTypeAsString();
				if (DEBUG) {
					System.out.println("field name " + field.getName() + " fieldType " + fieldType);
				}
				output.append("------------------ " + field.getName() + " -------------------------- \n");
				
				if (fieldType != null && fieldType.equals("TABLE")) {
					JCoTable table = field.getTable();
					JCoMetaData metaData = table.getMetaData();
					String actualTableName = metaData.getName();
					printMetaData(actualTableName, table, metaData);
				} else if (fieldType != null && fieldType.equals("STRUCTURE")) {
					JCoStructure val = field.getStructure();
					printStructure("", val);
				} else {
					System.out.println("Unexpected entry found: " + fieldType);
				}
			}
		}
		
		writeMetaDataToFile(functionName, output.toString());
		
	}
	
	private static void writeMetaDataToFile(String functionName, String str) {
		try {
			String actualFunctionName = functionName;
			
			if (functionName.lastIndexOf("/") != -1) {
				int startIndex = functionName.lastIndexOf("/") + 1;
				int endIndex = functionName.length();
				actualFunctionName = functionName.substring(startIndex , endIndex);
			}
			
			FileWriter file = new FileWriter(dir.getAbsolutePath() + File.separator + 
											actualFunctionName + "-metadata.txt");
			file.write(str);
			file.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	

	private static void printMetaData(String tableName, JCoTable table, JCoMetaData metaData) {
		int numCols = table.getNumColumns();
		for (int i = 0; i < numCols; i++) {
			String fieldName = metaData.getName(i);
			String desc = metaData.getDescription(i);
			String fieldType = metaData.getTypeAsString(i);
			output.append(fieldName + " : " + desc + "\n");

			// A Field can be another table or structure that needs to be investigated
			if (fieldType.equals("TABLE")) {
				if (table.getNumRows() > 0) {
					Object val = table.getValue(i);
					JCoTable internalTable = (JCoTable) val;
					printMetaDataInternalLevelOne("\t", internalTable);   
				}
			} else if (fieldType.equals("STRUCTURE")) {
				JCoStructure val = table.getStructure(i);
				printStructure("\t", val);
			} 
		}
	}

	private static void printMetaDataInternalLevelOne(String delim, JCoTable table) {
		int numCols = table.getNumColumns();
		JCoMetaData metaData = table.getMetaData();

		for (int i = 0; i < numCols; i++) {
			String fieldName = metaData.getName(i);
			String desc = metaData.getDescription(i);
			String fieldType = metaData.getTypeAsString(i);
			output.append(delim + fieldName + " : " + desc + "\n");


			if (DEBUG) {
				System.out.println(i);
				System.out.println("L1:Field Name: " + fieldName);
				System.out.println("L1:Description: " + desc);
				System.out.println("L1:Java Type: " + metaData.getClassNameOfField(i));
				System.out.println("L1:SAP Type: " + metaData.getRecordTypeName(i));
			}


			if (fieldType.equals("TABLE")) {
				if (table.getNumRows() > 0) {
					Object val = table.getValue(i);
					JCoTable internalTable = (JCoTable) val;
					printMetaDataInternalLevelTwo("\t\t",internalTable);   
				}
			} else if (fieldType.equals("STRUCTURE")) {
				JCoStructure val = table.getStructure(i);
				printStructure("\t\t", val);
			} 
		}
	}
		     
	private static void printMetaDataInternalLevelTwo(String delim, JCoTable table) {
		int numCols = table.getNumColumns();
		JCoMetaData metaData = table.getMetaData();

		for (int i = 0; i < numCols; i++) {
			String fieldName = metaData.getName(i);
			String desc = metaData.getDescription(i);
			//String fieldType = metaData.getTypeAsString(i);
			output.append(delim + fieldName + " : " + desc + "\n");
		}
	}

	private static void printStructure(String delim, JCoStructure structure) {
		JCoMetaData metaData = structure.getMetaData();
		int numFields = structure.getFieldCount();

		for (int i = 0; i < numFields; i++) {
			String fieldName = metaData.getName(i);
			String desc = metaData.getDescription(i);
			if (DEBUG) {
				System.out.println("fieldName: " + fieldName);
				System.out.println("typeAsString: " + metaData.getTypeAsString(i));
				System.out.println("description: " + desc);
				//String fieldType = metaData.getTypeAsString(i);
			}
			output.append(delim + fieldName + " : " + desc + "\n");
		}
	}
	
	private static void fetchMetrics() {
		JCoParameterList parameterList = function.getExportParameterList();
		if (parameterList != null) {
			// System.out.println("Displaying Export ParameterList");
			// System.out.println("Field Count " + parameterList.getFieldCount());
			JCoFieldIterator iterator = parameterList.getFieldIterator();
			while (iterator.hasNextField()) {
				JCoField field = iterator.nextField();
				String fieldType = field.getTypeAsString();
				if (fieldType != null && fieldType.equals("TABLE")) {
					JCoTable table = field.getTable();
					printJSON(field.getName(), table.toJSON());
				} else if (fieldType != null && fieldType.equals("STRUCTURE")) {
					JCoStructure val = field.getStructure();
					printJSON(field.getName(), val.toJSON());
				} else {
					System.out.println("Unexpected entry found: " + fieldType);
				}
			}
		}
		
		JCoParameterList tableParameterList = function.getTableParameterList();
		if (tableParameterList != null) {
			// System.out.println("Displaying Export ParameterList");
			// System.out.println("Field Count " + parameterList.getFieldCount());
			JCoFieldIterator iterator = tableParameterList.getFieldIterator();
			while (iterator.hasNextField()) {
				JCoField field = iterator.nextField();
				String fieldType = field.getTypeAsString();
				if (DEBUG) {
					System.out.println("field name " + field.getName() + " fieldType " + fieldType);
				}
				if (fieldType != null && fieldType.equals("TABLE")) {
					JCoTable table = field.getTable();
					printJSON(field.getName(), table.toJSON());
				} else if (fieldType != null && fieldType.equals("STRUCTURE")) {
					JCoStructure val = field.getStructure();
					printJSON(field.getName(), val.toJSON());
				} else {
					System.out.println("Unexpected entry found: " + fieldType);
				}
			}
		}

	}
	
	private static void printJSON(String fileName, String jsonPayload) {
		if (jsonPayload != null && !jsonPayload.equals("[]")) {
			try {
				FileWriter file = new FileWriter(dir.getAbsolutePath() + File.separator 
												+ fileName + ".json");
				
				// Using JSON parser
				
		        JsonNode jsonNode = mapper.readTree(jsonPayload);
		        String jsonStringWithoutQuotes = mapper.writeValueAsString(jsonNode);
				
				file.write(jsonStringWithoutQuotes);
				file.close();
				
				
				// Writing RAW JSON
				/*
				file.write(jsonPayload);
				file.close();
				*/
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
