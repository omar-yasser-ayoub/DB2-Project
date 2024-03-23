package org.example.data_structures.index;

import org.example.exceptions.DBAppException;
import org.example.data_structures.Table;

import java.io.*;

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
