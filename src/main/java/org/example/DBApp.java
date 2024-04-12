
/** * @author Wael Abouelsaadat */
package org.example;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.example.data_structures.SQLTerm;
import org.example.data_structures.Table;
import org.example.data_structures.Tuple;
import org.example.exceptions.DBAppException;
import org.example.managers.FileManager;
import org.example.managers.SelectionManager;

import java.io.*;
import java.util.*;


public class DBApp {

	public static final String METADATA_DIR = "data/table_metadata";
	public static int maxRowCount;
	static CSVWriter metadataWriter;

	static Vector<String> tables = new Vector<>();
	public DBApp( ){
		
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
		initMaxRowCount();
		initMetadataWriter();
	}

	private static void initMetadataWriter() {
		try {
			FileManager.createDirectory(METADATA_DIR);
			metadataWriter = new CSVWriter(new FileWriter(METADATA_DIR + "/metadata.csv", true),
					CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.RFC4180_LINE_END);

			CSVReader reader = new CSVReader(new FileReader(METADATA_DIR + "/metadata.csv"));
			String[] line = reader.readNext();

			// adding headers to start of file
			if(line == null){
				String[] header = { "Table Name", "Column Name", "Column Type", "ClusteringKey", "IndexName", "IndexType" };
				metadataWriter.writeNext(header);
			}

			// closing connection with writer
			metadataWriter.flush();
		} catch (Exception e) {
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
							Hashtable<String,String> htblColNameType) throws IOException, CsvValidationException, DBAppException {
		try {
			CSVReader reader = new CSVReader(new FileReader(METADATA_DIR));
			String[] line = reader.readNext();

			while ((line = reader.readNext()) != null) {
				if (strTableName.equals(line[0])) {
					throw new DBAppException("Table already exists");
				}
			}

			Table newTable = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
			newTable.save();
			writeMetadata(newTable);
			tables.add(newTable.getTableName());
		}
		catch (Exception e){
			System.out.println("Error while creating table");
		}
	}

	//** Writes the metadata of the table to the metadata file
	private void writeMetadata(Table table) throws IOException {
		CSVWriter writer = DBApp.metadataWriter;
		Hashtable<String, String> colNameType = table.getColNameType();
		Enumeration<String> columns = colNameType.keys();
		while (columns.hasMoreElements()){
			String column = columns.nextElement();
			String[] info = {table.getTableName(),
					column,
					colNameType.get(column),
					Objects.equals(table.getClusteringKey(), column) ? "True" : "False",
					"null",
					"null"};
			writer.writeNext(info);
			writer.flush();
		}
	}

	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException{
		
		try {
			Table table = FileManager.deserializeTable(strTableName);
			table.createIndex(strColName, strIndexName);
			table.save();
		}
		catch (Exception e){
			System.out.println("Error while creating index");
		}
	}


	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
								Hashtable<String,Object>  htblColNameValue) throws DBAppException{
		try{
			Tuple tuple = new Tuple(htblColNameValue);
			Table table = FileManager.deserializeTable(strTableName);
			table.insert(tuple);
			table.save();
		}
		catch (Exception e){
			System.out.println("Error while inserting into table");
			e.printStackTrace();
		}
	}


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException{
	
		deleteFromTable(strTableName, htblColNameValue);
		insertIntoTable(strTableName, htblColNameValue);
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException{

		try{
			Tuple tuple = new Tuple(htblColNameValue);
			Table table = FileManager.deserializeTable(strTableName);
			table.delete(tuple);
			table.save();
		}
		catch (Exception e){
			System.out.println("Error while inserting into table");
			e.printStackTrace();
		}
	}


	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
									String[]  strarrOperators) throws DBAppException{
		try {
			Iterator resultSet = SelectionManager.selectFromTable(arrSQLTerms, strarrOperators);
			return resultSet;
		}
		catch (Exception e){
			System.out.println("Error while selecting from table");
		}
		return null;
	}

	public static Vector<String> getMyTables() {
		return tables;
	}

	public static void main(String[] args ){
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

			//dbApp.createTable("CityShop", clusteringKey, ht);
			//dbApp.createTable("CityShop2", clusteringKey, ht);

			//SQLTerm[] arrSQLTerms = new SQLTerm[5];
			//arrSQLTerms[0] = new SQLTerm("CityShop", "Name", "=", "John Noor");

			//Testing.sqlTermTest();

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
//			arrSQLTerms[0].getStrTableName() =  "Student";
//			arrSQLTerms[0].getStrColumnName()=  "name";
//			arrSQLTerms[0].getStrOperator()  =  "=";
//			arrSQLTerms[0].getObjValue()     =  "John Noor";
//
//			arrSQLTerms[1].getStrTableName() =  "Student";
//			arrSQLTerms[1].getStrColumnName()=  "gpa";
//			arrSQLTerms[1].getStrOperator()  =  "=";
//			arrSQLTerms[1].getObjValue()     =  Double.valueOf( 1.5 );
//
//			String[] strarrOperators = new String[1];
//			strarrOperators[0] = "OR";
//			// select * from Student where name = "John Noor" or gpa = 1.5;
//			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		}
		catch(Exception exp){
			exp.printStackTrace();
		}
	}

}