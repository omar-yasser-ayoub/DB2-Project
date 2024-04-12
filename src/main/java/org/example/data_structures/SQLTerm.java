
/** * @author Wael Abouelsaadat */
package org.example.data_structures;



public class SQLTerm {

	private String strTableName;
	private String strColumnName;
	private String strOperator;
	private Object objValue;

	public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) {
		this.strTableName = strTableName;
		this.strColumnName = strColumnName;
		this.objValue = objValue;
		this.strOperator = strOperator;
	}
	public SQLTerm() {

	}

	public String getStrTableName() {
		return strTableName;
	}

	public String getStrColumnName() {
		return strColumnName;
	}

	public String getStrOperator() {
		return strOperator;
	}

	public Object getObjValue() {
		return objValue;
	}
	@Override
	public String toString() {
		return "SQLTerm{" +
				"strTableName='" + strTableName + '\'' +
				", strColumnName='" + strColumnName + '\'' +
				", strOperator='" + strOperator + '\'' +
				", objValue=" + objValue +
				'}';
	}
}