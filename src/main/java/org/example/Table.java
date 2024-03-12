package org.example;

import com.opencsv.CSVWriter;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Vector;

public class Table implements Serializable {
    String tableName;
    Hashtable<String,String> colNameType;
    String clusteringKey;
    Hashtable<String, String> indicesColNameType;
    Vector<Page> pages = new Vector<>();

    public Table(String tableName, String clusteringKey, Hashtable<String,String> colNameType){

        this.tableName = tableName;
        this.colNameType = colNameType;
        this.clusteringKey = clusteringKey;

        CSVWriter writer = new CSVWriter(DBApp.outputFile, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.RFC4180_LINE_END);
        Enumeration<String> e = colNameType.keys();
        while (e.hasMoreElements()){
            String key = e.nextElement();
            String[] info = {tableName,
                    key,
                    colNameType.get(key),
                    clusteringKey==key ? "True" : "False",
                    "null",
                    "null"};

            writer.writeNext(info);
        }
    }
    public void createPage() throws DBAppException {
        int pageNum = pages.size();
        Page newPage = new Page(this, pageNum);
        pages.add(newPage);
    }
    public void insertToTable(Tuple tuple) throws DBAppException {
        if(pages.isEmpty()){
            createPage();
        }
        isValidTuple(tuple);

        //TODO: Implement proper logic for finding the correct page to insert into
        pages.lastElement().insertIntoPage(tuple);
        //insert and update index
        //boolean isInsertCorrectly = currentPage.insert(tuple);
        //if (!isInsertCorrectly){
        //AddToPage(newPage);
        //logic for sorting
        //}

    }

    private void findPageToInsert(Tuple tuple){
        //binary search
    }

    private boolean isValidTuple(Tuple tuple) throws DBAppException {
        Hashtable<Object, Object> values = tuple.getValues();
        for (Object key : values.keySet()){
            //check if key is in colNameType
            if (!colNameType.containsKey(key)){
                throw new DBAppException("Key not found in table");
            }

            //check if value is of the correct type
            try {
                Class<?> expectedClass = Class.forName(colNameType.get(key));
                expectedClass.cast(values.get(key));
            } catch (ClassNotFoundException | ClassCastException ex) {
                throw new DBAppException("Key is not of the correct type");
            }

            //check if clustering key is unique
            if (key.equals(clusteringKey) && (!tupleHasNoDuplicateClusteringKey(key, values.get(key)))){
                throw new DBAppException("Clustering key is not unique");
            }

        }
        return true;
    }
    //TODO: Implement binary search
    private boolean tupleHasNoDuplicateClusteringKey(Object key, Object value){
        for (Page page : pages){
            if (page.tuples.isEmpty()){
                return true;
            }
            for (Tuple tuple : page.tuples){
                if (tuple.getValues().get(key).equals(value)){
                    return false;
                }
            }
        }
        return true;
    }

    public void createIndex(){
        //indexKey as attribute
        //create Index
    }
    public void updateIndex(){
        //update index
    }
}