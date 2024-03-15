
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

	public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) {
		_strTableName = strTableName;
		_strColumnName = strColumnName;
		_objValue = objValue;
		_strOperator = strOperator;
	}
	public SQLTerm() {

	}
}