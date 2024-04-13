package org.example.data_structures.index;

import org.example.data_structures.Tuple;
import org.example.exceptions.DBAppException;
import org.example.data_structures.Page;
import org.example.data_structures.Table;

import java.util.Vector;

import static org.example.managers.FileManager.deserializePage;

public class DoubleIndex extends Index {
    private BTree<Double,String> bTree;

    public DoubleIndex(Table parentTable, String columnName, String indexName){
        super(parentTable, columnName, indexName);
        this.bTree = new BTree<>();
    }

    public BTree<Double, String> getbTree() {
        return bTree;
    }

    @Override
    public boolean checkKeyExists(Object key) {
        return bTree.search((Double) key) != null;
    }

    public void populateIndex() throws DBAppException {
        Vector<String> pageNames = parentTable.getPageNames();
        for (String pageName : pageNames) {
            Page page = deserializePage(pageName);
            if (page.getTuples().isEmpty()) {
                continue;
            }
            for (Tuple tuple : page.getTuples()) {
                bTree.insert((Double) tuple.getValues().get(columnName), pageName);
            }
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
