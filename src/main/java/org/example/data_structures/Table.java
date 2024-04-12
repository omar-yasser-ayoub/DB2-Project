package org.example.data_structures;
import org.example.data_structures.index.DoubleIndex;
import org.example.data_structures.index.IntegerIndex;
import org.example.data_structures.index.StringIndex;
import org.example.exceptions.DBAppException;
import org.example.managers.DeletionManager;
import org.example.managers.FileManager;
import org.example.managers.InsertionManager;
import org.example.data_structures.index.Index;

import java.io.Serializable;
import java.util.*;

public class Table implements Serializable {
    private final String tableName;
    private final Hashtable<String,String> colNameType;
    private final String clusteringKey;
    private Vector<String> pageNames;
    private int pageCount;
    private String keyType;
    private Vector<String> indexNames;

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
    public boolean isEmpty() {
        return pageCount == 0;
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
        String columnType = colNameType.get(columnName);
        Index index = switch (columnType) {
            case "java.lang.Integer" -> new IntegerIndex(this, columnName, indexName);
            case "java.lang.String" -> new StringIndex(this, columnName, indexName);
            case "java.lang.Double" -> new DoubleIndex(this, columnName, indexName);
            default -> throw new IllegalArgumentException("Invalid column type");
        };
        index.populateIndex();
        this.indexNames.add(indexName);
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
        Hashtable<String, Object> values = tuple.getValues();
        for (String key : values.keySet()){
            //check if key is in colNameType
            if (!getColNameType().containsKey(key)){
                throw new DBAppException("Key not found in table");
            }

            //check if value is of the correct type
            try {
                Class<?> expectedClass = Class.forName(getColNameType().get(key));
                expectedClass.cast(values.get(key));
            } catch (ClassNotFoundException | ClassCastException ex) {
                throw new DBAppException("Key is not of the correct type");
            }
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
}