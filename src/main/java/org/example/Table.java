package org.example;

public class Table {
    String[] fileNames;//pages
    String[] columnNames;
    Page currentPage;

    //store relevant info about pages and serialize it
    public Table( String tableName, String[] columnName, String[] columnType, boolean[]clusteringKey, String[] indexName, String[] indexType){
        //writeMetadataToCSV();
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
