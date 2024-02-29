package org.example;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Table {
    String[] fileNames;//pages
    String[] columnNames;
    Page currentPage;

    //store relevant info about pages and serialize it
    public Table( String tableName, String[] columnName, String[] columnType, boolean[] clusteringKey, String[] indexName, String[] indexType){
        //writeMetadataToCSV();
            CSVWriter writer = new CSVWriter(DBApp.outputFile, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.RFC4180_LINE_END);

            for(int i = 0; i < columnName.length; i++){
                String[] info = {tableName,
                                 columnName[i],
                                 columnType[i],
                                 clusteringKey[i] ? "True" : "False",
                                 indexName[i],
                                 indexType[i]};
                writer.writeNext(info);
            }
        //Loop on tuple and insert in form
        // CityShop, ID, java.lang.Integer, True, IDIndex, B+tree
        this.columnNames = columnName;
    }
    public void createPage(){
        //when inserting
    }
    public void AddToPage(Tuple tuple){
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

    }
}
