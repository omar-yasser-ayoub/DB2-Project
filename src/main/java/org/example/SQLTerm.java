
/** * @author Wael Abouelsaadat */
package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class SQLTerm {

	public String _strTableName,_strColumnName, _strOperator;
	public Object _objValue;

	public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) throws DBAppException, IOException, CsvValidationException {
		// get datatype of value
		String type = "";
		if (objValue instanceof String) {
			type = "java.lang.String";
		}
		else if (objValue instanceof Integer) {
			type = "java.lang.Integer";
		}
		else if (objValue instanceof Double) {
			type = "java.lang.Double";
		}

		try {
			// create a reader
			CSVReader reader = new CSVReader(new FileReader("src/main/java/org/example/resources/metadata.csv"));
			String[] line = reader.readNext();

			// exception message stuff
			String exceptionMsg = "exceptionMsg";
			Boolean colInTable = false;
			Boolean tableExists = false;

			while((line = reader.readNext()) != null){
				if(strTableName.equals(line[0]) && strColumnName.equals(line[1])){
					exceptionMsg = "Object datatype doesn't match column datatype";
					colInTable = true;
					tableExists = true;
				}
				if(strTableName.equals(line[0]) && !strColumnName.equals(line[1]) && !colInTable){
					exceptionMsg = "Column does not exist in table";
					tableExists = true;
				}
				if(!tableExists)
					exceptionMsg = "Table does not exist";

				// if table exists and contains columns, and the datatype matches the column datatype, assign the values
				if(strTableName.equals(line[0]) && strColumnName.equals(line[1]) && type.equals(line[2])){
					_strTableName = strTableName;
					_strColumnName = strColumnName;
					_objValue = objValue;
					break;
				}
			}

			// if values were never assigned, that means condition was never met and exception should be thrown
			if(_strTableName == null)
				throw new DBAppException(exceptionMsg);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		catch (CsvValidationException e) {
			e.printStackTrace();
		}

		// check if operator is valid
		if(strOperator == ">" || strOperator == ">=" || strOperator == "<" || strOperator == "<=" || strOperator == "!=" || strOperator == "=")
			_strOperator = strOperator;
		else
			throw new DBAppException("Illegal operator");
	}

}