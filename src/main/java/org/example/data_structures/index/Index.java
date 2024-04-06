package org.example.data_structures.index;

import org.example.exceptions.DBAppException;
import org.example.data_structures.Table;

import java.io.*;

public abstract class Index implements Serializable {
    String indexName;
    String columnName;
    Table parentTable;
    protected Index(String indexName, Table parentTable, String columnName) {
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
    public abstract BTree getbTree();
    public abstract void populateIndex() throws DBAppException;
    public abstract void insert(Object key, String value);
    public abstract String search(Object key);
    public abstract void delete(Object key);
}
