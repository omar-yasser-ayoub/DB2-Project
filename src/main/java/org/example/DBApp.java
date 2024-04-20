/** * @author Wael Abouelsaadat */
package org.example;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.example.ANTLR.MySqlParserBaseListener;
import org.example.ANTLR.MySqlParserListener;
import org.example.data_structures.Page;
import org.example.data_structures.SQLTerm;
import org.example.data_structures.Table;
import org.example.data_structures.Tuple;
import org.example.exceptions.DBAppException;
import org.example.managers.ANTLRManager;
import org.example.managers.FileManager;
import org.example.managers.SelectionManager;
import org.example.managers.UpdateManager;
import org.example.ANTLR.MySqlLexer;
import org.example.ANTLR.MySqlParser;

import java.io.*;
import java.util.*;


public class DBApp {

	public static final String METADATA_DIR = "data/table_metadata";
	public static int maxRowCount;
	public static CSVWriter metadataWriter;

	static Vector<String> tables = new Vector<>();
	public DBApp( ) throws DBAppException {
		init();
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ) throws DBAppException {
		initMaxRowCount();
		initMetadataWriter();
		ANTLRManager.dbApp = this;
	}

	private static void initMetadataWriter() throws DBAppException {
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
			throw new DBAppException("Error while initialising metadata");
		}
	}

	private static void initMaxRowCount() throws DBAppException {
		Properties prop = new Properties();
		String fileName = "src/main/java/org/example/resources/DBApp.config";
		try (InputStream input = new FileInputStream(fileName)) {
			prop.load(input);
			maxRowCount = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		}
		catch (IOException e) {
			throw new DBAppException("Error while initialising max row count");
		}
	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary key and the clustering column as well.
	// The data type of that column will be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data type as value
	public void createTable(String strTableName, 
							String strClusteringKeyColumn,  
							Hashtable<String,String> htblColNameType) throws DBAppException {
		try {
			CSVReader reader = new CSVReader(new FileReader(METADATA_DIR + "/metadata.csv"));
			String[] line = reader.readNext();

			while ((line = reader.readNext()) != null) {
				if (strTableName.equals(line[0])) {
					throw new DBAppException("Table already exists");
				}
			}

			if(!htblColNameType.containsKey(strClusteringKeyColumn)) {
				throw new DBAppException("Clustering key not found in table");
			}

			if(strClusteringKeyColumn == null) {
				throw new DBAppException("Table must have a primary key");
			}

			Enumeration<String> keys = htblColNameType.keys();
			while (keys.hasMoreElements()) {
				String key = keys.nextElement();
				if(!(htblColNameType.get(key).equals("java.lang.String")
						|| htblColNameType.get(key).equals("java.lang.Integer")
						|| htblColNameType.get(key).equals("java.lang.Double"))) {
					throw new DBAppException("Invalid column datatype");
				}
			}

			Table newTable = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
			newTable.save();
			writeMetadata(newTable);
			tables.add(newTable.getTableName());
		}
		catch (Exception e){
			throw new DBAppException(e.getMessage());
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
			throw new DBAppException(e.getMessage());
		}
	}


	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
								Hashtable<String,Object>  htblColNameValue) throws DBAppException{
		if(htblColNameValue == null) {
			throw new DBAppException("htblColNameValue is null");
		}
		try{
			Table table = FileManager.deserializeTable(strTableName);
			Tuple tuple = new Tuple(htblColNameValue, table.getClusteringKey());
			table.insert(tuple);
			table.save();
		}
		catch (Exception e){
			throw new DBAppException(e.getMessage());
		}
	}


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue)  throws DBAppException{
		try {
			UpdateManager.updateTable(strTableName, strClusteringKeyValue, htblColNameValue);
		}
		catch (Exception e){
			throw new DBAppException(e.getMessage());
		}
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException{
		try{
			CSVReader reader = new CSVReader(new FileReader(METADATA_DIR + "/metadata.csv"));
			String[] line = reader.readNext();
			Vector<String> colNames = new Vector<>();
			while ((line = reader.readNext()) != null) {
				if (strTableName.equals(line[0])) {
					colNames.add(line[1]);
				}
			}
			if (colNames.isEmpty()) {
				throw new DBAppException("Table does not exist");
			}
			if (htblColNameValue == null || htblColNameValue.isEmpty()) {
				Table table = FileManager.deserializeTable(strTableName);
				table.wipePages();
				return;
			}
			Vector<SQLTerm> SQLTerms = new Vector<>();
			for (String colName: colNames) {
				if (htblColNameValue.get(colName) != null) {
					SQLTerms.add(new SQLTerm(strTableName, colName, "=", htblColNameValue.get(colName)));
					htblColNameValue.remove(colName);
				}
			}
			if (!htblColNameValue.isEmpty()) {
				throw new DBAppException("The tuple contains some columns that aren't in the table");
			}
			String[] strOperators = new String[SQLTerms.size() - 1];
			Arrays.fill(strOperators, "AND");
			SQLTerm[] SQLTermArray = SQLTerms.toArray(new SQLTerm[0]);
			Iterator<Tuple> resultSet = selectFromTable(SQLTermArray, strOperators);
			Table table = FileManager.deserializeTable(strTableName);
			while (resultSet.hasNext()) {
				Tuple element = resultSet.next();
				table.delete(element);
			}
			table.save();
		}
		catch (Exception e){
			throw new DBAppException(e.getMessage());
		}
	}

	public Iterator<Tuple> selectFromTable(SQLTerm[] arrSQLTerms,
									String[]  strarrOperators) throws DBAppException{
		try {
			strarrOperators = strarrOperators == null ? new String[0] : strarrOperators;
			Iterator resultSet = SelectionManager.selectFromTable(arrSQLTerms, strarrOperators);
			return resultSet;
		}
		catch (Exception e){
			throw new DBAppException(e.getMessage());
		}
	}

	public static Vector<String> getMyTables() {
		return tables;
	}

	// below method returns Iterator with result set if passed
	// strbufSQL is a select, otherwise returns null.
	public Iterator<Tuple> parseSQL(StringBuffer strbufSQL) throws DBAppException {
		ANTLRManager.checkValidSQL(strbufSQL);
		Vector<String> tokens = ANTLRManager.getTokenStrings(strbufSQL);

		return ANTLRManager.callMethod(tokens);
	}

	public static void main(String[] args ){
		try{
			DBApp dbApp = new DBApp();
			StringBuffer s = new StringBuffer();
			Iterator<Tuple> t;

			s.append("CREATE TABLE Customer (id int PRIMARY KEY, name varchar(50), address varchar(50), number int);");
			t = dbApp.parseSQL(s);

			s = new StringBuffer();
			s.append("CREATE INDEX name_idx ON Customer(id) USING BTREE;");
			t = dbApp.parseSQL(s);

			s = new StringBuffer();
			s.append("INSERT INTO Customer(id, name, address, number) VALUES(1, 'Farah', '123 street', 21);");
			t = dbApp.parseSQL(s);

			s = new StringBuffer();
			s.append("INSERT INTO Customer(id, name, address, number) VALUES(2, 'Omar', '456 street', 25);");
			t = dbApp.parseSQL(s);

			s = new StringBuffer();
			s.append("INSERT INTO Customer(id, name, address, number) VALUES(3, Ziad, '789 street', 12);");
			t = dbApp.parseSQL(s);

			s = new StringBuffer();
			s.append("INSERT INTO Customer(id, name, address, number) VALUES(4, 'Yara', '123 lane', 52);");
			t = dbApp.parseSQL(s);

//			s.append("DELETE FROM Customer;");
//			dbApp.parseSQL(s);

//			s = new StringBuffer();
//			s.append("SELECT * FROM Customer WHERE name = 'Farah' OR name = 'Omar'");
//			t = dbApp.parseSQL(s);
//			while(t.hasNext())
//			{
//				Tuple tuple = t.next();
//				System.out.println(tuple.toString());
//			}

			s = new StringBuffer();
			s.append("UPDATE Customer SET number = 100 WHERE id = 3;");
			t = dbApp.parseSQL(s);

			Table table = FileManager.deserializeTable("Customer");
			Page page = FileManager.deserializePage(table.getPageNames().get(0));
			System.out.println(page.toString());

//			Hashtable<String, String> ht = new Hashtable<>();
//			ht.put("a", "java.lang.String");
//			ht.put("prim", "java.lang.String");
//			dbApp.createTable("test", "prim", ht);
//			dbApp.createIndex("test", "wah", "testIndex");
			//Testing.sqlTermTest();
      
			Hashtable htblColNameType = new Hashtable( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");

			dbApp.createTable("Student", "id", htblColNameType);
			dbApp.createTable("Admin", "id", htblColNameType);

			dbApp.createIndex("Student","id","id");
			dbApp.createIndex("Student","name","name");
			dbApp.createIndex("Admin","name","name");

			Hashtable htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", new Integer( 2343432 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			dbApp.insertIntoTable( "Student" , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("name", new String("Mr. Noor" ) );
			htblColNameValue.put("gpa", new Double( 1.00 ) );
			dbApp.updateTable("Student", "2343432", htblColNameValue);

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 2343432 ));
			//dbApp.deleteFromTable("Student", null);

			System.out.println(FileManager.deserializeTable("Student"));
		}
		catch(Exception exp) {
			exp.printStackTrace();
		}
	}

}