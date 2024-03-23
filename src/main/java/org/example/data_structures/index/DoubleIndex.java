package org.example.data_structures.index;

import org.example.exceptions.DBAppException;
import org.example.data_structures.Page;
import org.example.data_structures.Table;

import java.util.Vector;

import static org.example.managers.FileManager.deserializePage;

public class DoubleIndex implements Index {

    private final String columnType;
    private final String columnName;
    private final Table parentTable;
    private BTree<Double,String> bTree;


    public DoubleIndex(Table parentTable, String columnType, String columnName) throws IllegalArgumentException {
        this.columnType = columnType;
        this.parentTable = parentTable;
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public Table getParentTable() {
        return parentTable;
    }

    public BTree<Double, String> getbTree() {
        return bTree;
    }


    public void populateIndex() throws DBAppException {
        Vector<String> pageNames = parentTable.getPageNames();
        for (String pageName : pageNames) {
            Page page = deserializePage(pageName);
            if (page.getTuples().isEmpty()) {
                continue;
            }
            bTree.insert((Double) page.getTuples().get(0).getValues().get(columnName), pageName);
        }
    }

    @Override
    public void insert(Object key, String value) {
        bTree.insert((Double) key, value);
    }

    @Override
    public String search(Object key) {
        return bTree.search((Double) key);
    }

    @Override
    public void delete(Object key) {
        bTree.delete((Double) key);
    }

}
