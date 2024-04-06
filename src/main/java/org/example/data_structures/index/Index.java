package org.example.data_structures.index;

import org.example.exceptions.DBAppException;
import org.example.data_structures.Table;
import org.example.managers.FileManager;

import java.io.*;

public abstract class Index implements Serializable {
    String indexName;
    String columnName;
    Table parentTable;

    //TODO: Check if no indices exist with same name
    protected Index(Table parentTable, String columnName, String indexName) {
        this.indexName = indexName;
        this.parentTable = parentTable;
        this.columnName = columnName;
    }
    public String getIndexName() { return indexName; }
    public String getColumnName() {
        return columnName;
    }
    public Table getParentTable() {
        return parentTable;
    }
    public void save() throws DBAppException {
        FileManager.serializeIndex(this);
    }
    public abstract BTree getbTree();
    public abstract void populateIndex() throws DBAppException;
    public abstract void insert(Object key, String value);
    public abstract String search(Object key);
    public abstract void delete(Object key);
}
