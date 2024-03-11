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
    Vector<Page> pages;
    Page currentPage;

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
    public void createPage(){
        //when inserting
    }
    public void addToPage(Tuple tuple){
        //insert and update index
        //boolean isInsertCorrectly = currentPage.insert(tuple);
        //if (!isInsertCorrectly){
        //AddToPage(newPage);
        //logic for sorting
        //}

    }
    public void createIndex(){
        //indexKey as attribute
        //create Index
    }
    public void updateIndex(){
        //update index
    }
}