package org.example;

import java.io.*;
import java.util.Vector;

import static org.example.FileManager.deserializePage;

public interface Index extends Serializable {
    public String getColumnType();
    public String getColumnName();
    public Table getParentTable();
    public BTree getbTree();
    public void populateIndex() throws DBAppException;
    public void insert(Object key, String value);
    public String search(Object key);
    public void delete(Object key);
}
