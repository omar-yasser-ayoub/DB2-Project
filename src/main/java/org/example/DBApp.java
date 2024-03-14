
/** * @author Wael Abouelsaadat */
package org.example;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.*;
import java.util.Iterator;
import java.util.Hashtable;
import java.util.Properties;


public class DBApp {

	public static FileWriter outputFile;
	public static int maxRowCount;
	static CSVWriter writer;


	public DBApp( ){
		
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
		initMaxRowCount();
		initFileWriter();
	}

	private static void initFileWriter() {
		try {
			// creating file and writer
			outputFile = new FileWriter("src/main/java/org/example/resources/metadata.csv");
			writer = new CSVWriter(outputFile, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.RFC4180_LINE_END);

			// adding headers to start of file
			String[] header = { "Table Name", "Column Name", "Column Type", "ClusteringKey", "IndexName", "IndexType" };
			writer.writeNext(header);

			// closing connection with writer
			// writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void initMaxRowCount(){
		Properties prop = new Properties();
		String fileName = "src/main/java/org/example/resources/DBApp.config";
		try (InputStream input = new FileInputStream(fileName)) {
			prop.load(input);
			maxRowCount = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}


	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary key and the clustering column as well.
	// The data type of that column will be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data type as value


	public void createTable(String strTableName, 
							String strClusteringKeyColumn,  
							Hashtable<String,String> htblColNameType) throws DBAppException{

		Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		throw new DBAppException("not implemented yet");
	}















	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException{
		
		throw new DBAppException("not implemented yet");
	}


	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
								Hashtable<String,Object>  htblColNameValue) throws DBAppException{
	
		Tuple tuple = new Tuple(htblColNameValue);
		//TODO: Load table from disk
		//Table table = loadTableFromCSV(strTableName);
	}


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException{
	
		throw new DBAppException("not implemented yet");
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException{
	
		throw new DBAppException("not implemented yet");
	}


	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, 
									String[]  strarrOperators) throws DBAppException{
										
		return null;
	}


	public static void main( String[] args ){
	try{
			DBApp dbApp = new DBApp();
			dbApp.init();

			String[] colNames = {"ID", "Name", "Number", "Specialisation", "Address"};
			String[] colTypes = {"java.lang.Integer", "java.lang.String", "java.lang.Integer", "java.lang.String", "java.lang.String"};
			Hashtable<String, String> ht = new Hashtable<>();
			for(int i = 0; i < colNames.length; i++){
				ht.put(colNames[i], colTypes[i]);
			}
			String clusteringKey = "ID";
			String[] indexName = {"IDIndex", null, "NumberIndex", "SpecIndex", "AddrIndex"};
			String[] indexType = {"B+tree", null, "B+tree", "B+tree", "B+tree"};
			Table test = new Table("CityShop", clusteringKey, ht);
			Table test2 = new Table("CityShop2", clusteringKey, ht);

			SQLTerm[] arrSQLTerms = new SQLTerm[5];
			arrSQLTerms[0] = new SQLTerm("CityShop", "Name", "=", "John Noor");

//			String strTableName = "Student";
//			Hashtable htblColNameType = new Hashtable( );
//			htblColNameType.put("id", "java.lang.Integer");
//			htblColNameType.put("name", "java.lang.String");
//			htblColNameType.put("gpa", "java.lang.double");
//			dbApp.createTable( strTableName, "id", htblColNameType );
//			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
//
//			Hashtable htblColNameValue = new Hashtable( );
//			htblColNameValue.put("id", Integer.valueOf(2343432));
//			htblColNameValue.put("name", new String("Ahmed Noor" ) );
//			htblColNameValue.put("gpa", Double.valueOf( 0.95 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", Integer.valueOf( 453455 ));
//			htblColNameValue.put("name", new String("Ahmed Noor" ) );
//			htblColNameValue.put("gpa", Double.valueOf( 0.95 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", Integer.valueOf( 5674567 ));
//			htblColNameValue.put("name", new String("Dalia Noor" ) );
//			htblColNameValue.put("gpa", Double.valueOf( 1.25 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", Integer.valueOf( 23498 ));
//			htblColNameValue.put("name", new String("John Noor" ) );
//			htblColNameValue.put("gpa", Double.valueOf( 1.5 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", Integer.valueOf( 78452 ));
//			htblColNameValue.put("name", new String("Zaky Noor" ) );
//			htblColNameValue.put("gpa", Double.valueOf( 0.88 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//
//			SQLTerm[] arrSQLTerms;
//			arrSQLTerms = new SQLTerm[2];
//			arrSQLTerms[0]._strTableName =  "Student";
//			arrSQLTerms[0]._strColumnName=  "name";
//			arrSQLTerms[0]._strOperator  =  "=";
//			arrSQLTerms[0]._objValue     =  "John Noor";
//
//			arrSQLTerms[1]._strTableName =  "Student";
//			arrSQLTerms[1]._strColumnName=  "gpa";
//			arrSQLTerms[1]._strOperator  =  "=";
//			arrSQLTerms[1]._objValue     =  Double.valueOf( 1.5 );
//
//			String[] strarrOperators = new String[1];
//			strarrOperators[0] = "OR";
//			// select * from Student where name = "John Noor" or gpa = 1.5;
//			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		}
		catch(Exception exp){
			exp.printStackTrace();
		}
		finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}