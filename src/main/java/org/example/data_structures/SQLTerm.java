
/** * @author Wael Abouelsaadat */
package org.example.data_structures;



public class SQLTerm {

	public String _strTableName;
	public String _strColumnName;
	public String _strOperator;
	public Object _objValue;

	public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) {
		this._strTableName = strTableName;
		this._strColumnName = strColumnName;
		this._objValue = objValue;
		this._strOperator = strOperator;
	}
	public SQLTerm() {

	}

	public String getStrTableName() {
		return _strTableName;
	}

	public String getStrColumnName() {
		return _strColumnName;
	}

	public String getStrOperator() {
		return _strOperator;
	}

	public Object getObjValue() {
		return _objValue;
	}
	@Override
	public String toString() {
		return "SQLTerm{" +
				"strTableName='" + _strTableName + '\'' +
				", strColumnName='" + _strColumnName + '\'' +
				", strOperator='" + _strOperator + '\'' +
				", objValue=" + _objValue +
				'}';
	}
}