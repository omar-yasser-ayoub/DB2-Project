package org.example.data_structures;

import org.example.DBApp;
import org.example.exceptions.DBAppException;
import org.example.managers.FileManager;

import java.io.*;
import java.util.Vector;

public class Page implements Serializable {
    private int pageNum;
    private Vector<Tuple> tuples;
    private String parentTableName;

    /**
     * Constructor for the Page class
     * @param parentTable The table that the page is a part of
     * @param pageNum The identifying number of the page
     */
    public Page(Table parentTable, int pageNum){
        this.parentTableName = parentTableName;
        this.pageNum = pageNum;
        this.tuples = new Vector<>();
    }

    public int getPageNum() {
        return pageNum;
    }

    public Vector<Tuple> getTuples() {
        return tuples;
    }

    public Table getParentTable() throws DBAppException {
        Table parentTable = FileManager.deserializeTable(parentTableName);
        return parentTable;
    }

    public String getPageName() {
        return parentTableName + getPageNum();
    }

    /**
     * Serializes the page object to the disk
     * @throws DBAppException If an error occurs during serialization
     */
    public void save() throws DBAppException {
        FileManager.serializePage(this);
    }

    public String toString() {
        if (getTuples() == null || getTuples().isEmpty()) {
            return "Page is Empty";
        }
        StringBuilder returnString = new StringBuilder();
        for (Object tuple : getTuples()) {
            returnString.append(tuple.toString());
            returnString.append(",");
        }
        returnString.deleteCharAt(returnString.length() - 1);
        return returnString.toString();
    }

    public int getSize() {
        return tuples.size();
    }

    public boolean isFull() {
        return tuples.size() == DBApp.maxRowCount;
    }
}
