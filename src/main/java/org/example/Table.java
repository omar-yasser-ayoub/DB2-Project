package org.example;

import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static org.example.FileManager.deserializePage;

public class Table implements Serializable {
    private String tableName;
    private Hashtable<String,String> colNameType;
    private String clusteringKey;
    private Hashtable<String, String> indicesColNameType;
    private Vector<String> pageNames;
    private int pageCount;
    private String keyType;
    private Object index;

    public Table(String tableName, String clusteringKey, Hashtable<String,String> colNameType) throws IOException, CsvValidationException, DBAppException {
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

    public Hashtable<String, String> getIndicesColNameType() {
        return indicesColNameType;
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

    public Object getIndex() {
        return index;
    }

    public void setIndex(Object index) {
        this.index = index;
    }

    public void insert(Tuple tuple) throws DBAppException {
        InsertionManager.insertTupleIntoTable(tuple, this);
    }

    public void delete(Tuple tuple) throws DBAppException {
        DeletionManager.deleteFromTable(tuple, this);
    }

    /**
     * Creates a new page and adds it to the table
     * @return The newly created page
     */
    public Page createPageInTable() throws DBAppException {
        Page newPage = new Page(this, getPageCount());
        newPage.save();
        String pageName = getTableName() + getPageCount();
        getPageNames().add(pageName);
        pageCount = getPageCount() + 1;
        return newPage;
    }
    /**
     * Creates a new page with a tuple, and adds it to the table at the specified index
     * @param tuple The tuple to be inserted into the page
     * @param index The index at which the page is to be inserted
     * @return The newly created page
     */
    public Page createPageInTable(Tuple tuple, int index) throws DBAppException {
        Page newPage = new Page(this, getPageCount());
        newPage.save();
        String pageName = getTableName() + getPageCount();
        getPageNames().add(index + 1, pageName);
        InsertionManager.insertTupleIntoPage(tuple, newPage);
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

    public Vector<Tuple> linearSearch(SQLTerm Term) throws DBAppException {
        Vector<Tuple> finalList = new Vector<Tuple>();
        for (String pageName : getPageNames()) {
            Page page = deserializePage(pageName);
            if (page.getTuples().isEmpty()){
                continue;
            }
            for (Tuple tuple : page.getTuples()) {
                switch (Term._strOperator) {
                    case ">":
                        if (Term._objValue instanceof String) {
                            int value = ((String) tuple.getValues().get(Term._strColumnName)).compareTo((String) Term._objValue);
                            if (value > 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Integer) {
                            int value = ((Integer) tuple.getValues().get(Term._strColumnName)).compareTo((Integer) Term._objValue);
                            if (value > 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Double) {
                            int value = ((Double) tuple.getValues().get(Term._strColumnName)).compareTo((Double) Term._objValue);
                            if (value > 0) {
                                finalList.add(tuple);
                            }
                        }
                        break;
                    case ">=":
                        if (Term._objValue instanceof String) {
                            int value = ((String) tuple.getValues().get(Term._strColumnName)).compareTo((String) Term._objValue);
                            if (value > 0 || value == 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Integer) {
                            int value = ((Integer) tuple.getValues().get(Term._strColumnName)).compareTo((Integer) Term._objValue);
                            if (value > 0 || value == 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Double) {
                            int value = ((Double) tuple.getValues().get(Term._strColumnName)).compareTo((Double) Term._objValue);
                            if (value > 0 || value == 0) {
                                finalList.add(tuple);
                            }
                        }
                        break;
                    case "<":
                        if (Term._objValue instanceof String) {
                            int value = ((String) tuple.getValues().get(Term._strColumnName)).compareTo((String) Term._objValue);
                            if (value < 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Integer) {
                            int value = ((Integer) tuple.getValues().get(Term._strColumnName)).compareTo((Integer) Term._objValue);
                            if (value < 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Double) {
                            int value = ((Double) tuple.getValues().get(Term._strColumnName)).compareTo((Double) Term._objValue);
                            if (value < 0) {
                                finalList.add(tuple);
                            }
                        }
                        break;
                    case "<=":
                        if (Term._objValue instanceof String) {
                            int value = ((String) tuple.getValues().get(Term._strColumnName)).compareTo((String) Term._objValue);
                            if (value < 0 || value == 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Integer) {
                            int value = ((Integer) tuple.getValues().get(Term._strColumnName)).compareTo((Integer) Term._objValue);
                            if (value < 0 || value == 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Double) {
                            int value = ((Double) tuple.getValues().get(Term._strColumnName)).compareTo((Double) Term._objValue);
                            if (value < 0 || value == 0) {
                                finalList.add(tuple);
                            }
                        }
                        break;
                    case "=":
                        if (tuple.getValues().get(Term._strColumnName).equals(Term._objValue)) {
                            finalList.add(tuple);
                        }
                        break;
                    case "!=":
                        if (!tuple.getValues().get(Term._strColumnName).equals(Term._objValue)) {
                            finalList.add(tuple);
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        return finalList;
    }

}