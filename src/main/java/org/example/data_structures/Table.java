package org.example.data_structures;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.example.DBApp;
import org.example.data_structures.index.DoubleIndex;
import org.example.data_structures.index.IntegerIndex;
import org.example.data_structures.index.StringIndex;
import org.example.exceptions.DBAppException;
import org.example.managers.DeletionManager;
import org.example.managers.FileManager;
import org.example.managers.InsertionManager;
import org.example.data_structures.index.Index;

import java.io.*;
import java.sql.SQLOutput;
import java.util.*;

import static org.example.DBApp.METADATA_DIR;
import static org.example.DBApp.metadataWriter;

public class Table implements Serializable {
    private final String tableName;
    private final Hashtable<String,String> colNameType;
    private final String clusteringKey;
    private Vector<String> pageNames;
    private int pageCount;
    private String keyType;
    private Vector<String> indexNames = new Vector<>();
    public Hashtable <String, String> isIndexCreatedOnColumn = new Hashtable<>();

    public Table(String tableName, String clusteringKey, Hashtable<String,String> colNameType){
        this.tableName = tableName;
        this.colNameType = colNameType;
        this.clusteringKey = clusteringKey;
        this.pageNames = new Vector<>();
        this.pageCount = 0;
    }

    public String getTableName() {
        return tableName;
    }

    public Hashtable<String, String> getColNameType() {
        return colNameType;
    }

    public String getClusteringKey() {
        return clusteringKey;
    }
    public boolean isEmpty() throws DBAppException {
        return this.getSize() == 0;
    }
    public Vector<String> getPageNames() {
        return pageNames;
    }

    public int getPageCount() {
        return pageCount;
    }

    public String getKeyType() {
        return keyType;
    }

    public Vector<String> getIndices() {
        return indexNames;
    }

    public void createIndex(String columnName, String indexName) throws DBAppException {
        if (this.isIndexCreatedOnColumn.get(columnName) != null || indexNames.contains(indexName)) {
            throw new DBAppException("The index was already created on one of the columns");
        }
        String columnType = colNameType.get(columnName);
        Index index = switch (columnType) {
            case "java.lang.Integer" -> new IntegerIndex(this, columnName, indexName);
            case "java.lang.String" -> new StringIndex(this, columnName, indexName);
            case "java.lang.Double" -> new DoubleIndex(this, columnName, indexName);
            default -> throw new IllegalArgumentException("Invalid column type");
        };
        index.populateIndex();
        this.indexNames.add(indexName);
        editMetadata(columnName, indexName);
        FileManager.serializeIndex(index);
        this.isIndexCreatedOnColumn.put(columnName, indexName);
    }

    private void editMetadata(String columnName, String indexName) throws DBAppException {
        try {
            CSVReader reader = new CSVReader(new FileReader(METADATA_DIR + "/metadata.csv"));
            String[] line = reader.readNext();
            Vector<String[]> csv = new Vector<>();
            csv.add(line);

            while((line = reader.readNext()) != null) {
                if(line[0].equals(tableName)) {
                    if(line[1].equals(columnName)) {
                        line[4] = indexName;
                        line[5] = "B+tree";
                    }
                }
                csv.add(line);
            }

            metadataWriter.close();

            metadataWriter = new CSVWriter(new FileWriter(METADATA_DIR + "/metadata.csv", false),
                    CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.RFC4180_LINE_END);

            CSVWriter writer = metadataWriter;
            writer.writeAll(csv);
            writer.flush();

            metadataWriter = new CSVWriter(new FileWriter(METADATA_DIR + "/metadata.csv", true),
                    CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.RFC4180_LINE_END);

        } catch (Exception e) {
            throw new DBAppException("Error while editing metadata");
        }
    }

    public void insert(Tuple tuple) throws DBAppException {
        InsertionManager.insertTupleIntoTable(tuple, this);
    }

    public void delete(Tuple tuple) throws DBAppException {
        DeletionManager.deleteFromTable(tuple, this);
    }

    public void save() throws DBAppException {
        FileManager.serializeTable(this);
    }

    /**
     * Creates a new page and adds it to the table
     * @return The newly created page
     */
    public Page createPageInTable() throws DBAppException {
        Page newPage = new Page(tableName, getPageCount());
        newPage.save();

        String pageName = getTableName() + getPageCount();
        getPageNames().add(pageName);
        pageCount = getPageCount() + 1;

        this.save();
        return newPage;
    }
    /**
     * Creates a new page with a tuple, and adds it to the table at the specified index
     * @param tuple The tuple to be inserted into the page
     * @param index The index at which the page is to be inserted
     * @return The newly created page
     */
    public Page createPageInTable(Tuple tuple, int index) throws DBAppException {
        Page newPage = new Page(tableName, getPageCount());
        String pageName = getTableName() + getPageCount();

        pageNames.add(index + 1, pageName);
        InsertionManager.insertTupleIntoPage(tuple, this, newPage);


        this.save();
        return newPage;
    }

    public boolean isValidTuple(Tuple tuple) throws DBAppException {
        Hashtable<String, Object> values = new Hashtable<>();
        Enumeration<String> keys = tuple.getValues().keys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            values.put(key, tuple.getValues().get(key));
        }

        try {
            CSVReader reader = new CSVReader(new FileReader(METADATA_DIR + "/metadata.csv"));
            String[] line = reader.readNext();

            while ((line = reader.readNext()) != null) {
                if(line[0].equals(tableName)) {
                    if (values.containsKey(line[1])) {
                        String type = line[2];
                        if(!((line[2].equals("java.lang.String") && values.get(line[1]) instanceof String)
                            || (line[2].equals("java.lang.Integer") && values.get(line[1]) instanceof Integer)
                            || (line[2].equals("java.lang.Double") && values.get(line[1]) instanceof Double))) {
                            throw new DBAppException("Value is not of the correct type");
                        }
                        values.remove(line[1]);
                    }
                    else if (line[3].equals("True")) {
                        throw new DBAppException("Tuple must contain primary key");
                    }
                }
            }

            if(values.size() > 0) {
                throw new DBAppException("Key is not found in table");
            }

        } catch (CsvValidationException | IOException e) {
            throw new DBAppException(e.getMessage());
        }

        return true;
    }

    public Page getPageAtPosition(int i) throws DBAppException {
        String currentPage = pageNames.get(i);
        return FileManager.deserializePage(currentPage);
    }

    public int getSize() throws DBAppException {
        int total = 0;
        for (String pageName : pageNames) {
            total += FileManager.deserializePage(pageName).getSize();
        }
        return total;
    }
    public void wipePages() throws DBAppException {
        for (String pageName : pageNames) {
            Page page = FileManager.deserializePage(pageName);
            page.clearTuples();
        }
        pageNames.clear();
        pageCount = 0;

    }
    public String toString() {
        StringBuilder returnString = new StringBuilder();
        for (String pageName : pageNames) {
            try {
                Page page = FileManager.deserializePage(pageName);
                returnString.append(page.toString());
            } catch (DBAppException e) {
                e.printStackTrace();
            }
        }
        return returnString.toString();
    }
}