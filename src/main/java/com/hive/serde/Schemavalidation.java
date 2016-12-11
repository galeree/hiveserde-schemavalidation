package com.hive.serde;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

// Create a class that implements SerDe interface
public class Schemavalidation implements SerDe
{
	// Set default separator to tab
	private String defaultSep = "\t";
	
	// Important variables
	private StructTypeInfo rowTypeInfo;
	private ObjectInspector rowOI;
	private List<String> colNames;
	private List<Object> row = new ArrayList<Object>();
	private String separatorChar;

	// Setup important property
	public void initialize(Configuration conf, Properties tbl)
			throws SerDeException {
		// Get all column name in comma delimeted string
		String colNamesStr = tbl.getProperty(Constants.LIST_COLUMNS);
		
		// Create column list from column name string
		colNames = Arrays.asList(colNamesStr.split(","));
		
		// Get field separator character
		separatorChar = getSeparator(tbl, defaultSep);
		
		// Get all column type in string
		String colTypeStr = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
		
		// Create list of TypeInfo from column type string
		List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypeStr);
		
		// Create rowTypInfo from column name list and TypeInfo list
		rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
		
		// Create object inspector in order to serialize java object for each row
		rowOI = (StructObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
	}
	
	// Get field separator character
	public String getSeparator(Properties prop, String defaultSep) {
		final String value = prop.getProperty("separatorChar");
		if (value != null) {
			return value;
		}
		return defaultSep;
	}
	
	// Convert writable object from file to primitive java object 
	public Object deserialize(Writable blob) throws SerDeException {
		
		// Clear data
		row.clear();
		
		// Cast writable object to Text
		Text rowText = (Text) blob;
		
		// Create array of string from row string
		String[] rowStr = rowText.toString().split(Pattern.quote(separatorChar));
		
		// Create container to store object for each field
		Object value = null;
		int index = 0;
		
		// Iterate over all fields
		for (String fieldName : rowTypeInfo.getAllStructFieldNames()) {
			
			// Display exception message if have problem in getting field value
			try {
				// Get TypeInfo for each field
				TypeInfo fieldTypeInfo = rowTypeInfo.getStructFieldTypeInfo(fieldName);
				String field = rowStr[index];
				
				// Get field value
				value = parseField(field, fieldTypeInfo);
				
				// Add field value to row object
				row.add(value);
			} catch (Exception e) {
				// Throw exception
				throw new SerDeException(e.getMessage());
			}
			index += 1;
		}
		return row;
	}
	
	// Method to get field data
	public Object parseField(String field, TypeInfo fieldTypeInfo) throws Exception {
		
		// Get field type in string format if field type is decimal(X,X) substring it to decimal
		String fieldType = fieldTypeInfo.getTypeName();
		if (fieldType.contains("(")) {
			fieldType = fieldType.substring(0,fieldType.indexOf("("));
		}
		
		// Get exception message
		String message = String.format("<%s> is not of type %s", field, fieldType.toUpperCase());
		
		// If field value is blank or space and field type is not string then return null 
		if(fieldType.equals(Constants.STRING_TYPE_NAME) && field.trim().equals("")) {
			return null;
		}
		
		// Check field's data type
		try {
			switch(fieldType) {
				case Constants.STRING_TYPE_NAME:
					return field;
				case Constants.INT_TYPE_NAME:
					return Integer.parseInt(field);
				case Constants.SMALLINT_TYPE_NAME:
					return Integer.parseInt(field);
				case Constants.TINYINT_TYPE_NAME:
					return Integer.parseInt(field);
				case Constants.BIGINT_TYPE_NAME:
					return Long.parseLong(field);
				case Constants.DOUBLE_TYPE_NAME:
					return Double.parseDouble(field);
				case Constants.FLOAT_TYPE_NAME:
					return Float.parseFloat(field);
				case "decimal":
					return HiveDecimal.create(new BigDecimal(field));
				case Constants.BOOLEAN_TYPE_NAME:
					switch(field) {
						case "0":
							return false;
						case "false":
							return false;
						case "1":
							return true;
						case "true":
							return true;
						default:
							throw new Exception(message);
					}
				case Constants.TIMESTAMP_TYPE_NAME:
					SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
					fmt.setLenient(true);
					Date date = fmt.parse(field);
					return new Timestamp(date.getTime());
				case Constants.DATE_TYPE_NAME:
					SimpleDateFormat fmd = new SimpleDateFormat("yyyy-MM-dd");
					fmd.setLenient(true);
					return new java.sql.Date(fmd.parse(field).getTime());
				default:
					return field;
			}
		} catch (Exception e) {
			throw new Exception(message);
		}
	}
	
	// Serialize java object to writable object
	public Writable serialize(Object obj, ObjectInspector oi) throws SerDeException {
		
		// Convert ObjectInspector to StructObjectInspector
		StructObjectInspector soi = (StructObjectInspector) oi;
		
		// Create a list to store result data
		List<Object> resultList = new ArrayList<Object>();
		
		// Use ObjectInspector to get value of each field from object
		for (StructField field : soi.getAllStructFieldRefs()) {	
			Object item = soi.getStructFieldData(obj, field);
			// Add field value to a result list that store row's data
			resultList.add(item);
		}
		
		// Create separatorChar separated field from row's list
		String delimeter = "";
		StringBuilder sb = new StringBuilder();
		for (Object item : resultList) {
		    sb.append(delimeter).append(item.toString());
		    delimeter = separatorChar;
		}
		
		// Create and return Writable Text object from string
		return new Text(sb.toString());
	}
	
	// Get ObjectInspector object
	public ObjectInspector getObjectInspector() throws SerDeException {
		return rowOI;
	}

	public SerDeStats getSerDeStats() {
		return null;
	}

	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}
}
