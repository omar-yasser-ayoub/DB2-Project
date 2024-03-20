package org.example;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.Vector;

import static org.example.Page.deserializePage;

public class Index implements Serializable {
    BTree<Integer,String> integerIndex;
    BTree<String,String> stringIndex;
    BTree<Double,String> doubleIndex;
    String _columnType;
    String _columnName;
    Table _parentTable;

    public Index(Table parentTable, String columnType, String columnName) throws IllegalArgumentException {
        this._columnType = columnType;
        this._parentTable = parentTable;
        this._columnName = columnName;
        switch (columnType) {
            case "java.lang.String" -> stringIndex = new BTree<String, String>();
            case "java.lang.Integer" -> integerIndex = new BTree<Integer, String>();
            case "java.lang.Double" -> doubleIndex = new BTree<Double, String>();
            default -> throw new IllegalArgumentException("Unsupported column type: " + columnType);
        }
    }

    public void serializeIndex() throws DBAppException {
        createSerializedDirectory();
        String fileName = "data/serialized_indices/" + _parentTable.tableName + _columnName + ".ser";
        try (FileOutputStream fileOut = new FileOutputStream(fileName); ObjectOutputStream objOut = new ObjectOutputStream(fileOut)) {
            objOut.writeObject(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createSerializedDirectory() throws DBAppException {
        String directoryPath = "data/serialized_indices";
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (!created) {
                throw new DBAppException("Failed to create directory: " + directoryPath);
            }
        }
    }

    public static Index deserializeIndex(String indexName) throws DBAppException {
        String fileName = "data/serialized_indices/" + indexName + ".ser";
        try (FileInputStream fileIn = new FileInputStream(fileName); ObjectInputStream objIn = new ObjectInputStream(fileIn)) {
            return  (Index) objIn.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public void populateIndex() throws DBAppException {
        Vector<String> pageNames = _parentTable.pageNames;
        for (String pageName : pageNames) {
            Page page = deserializePage(pageName);
            if (page.tuples.isEmpty()) {
                continue;
            }
            switch (_columnType) {
                case "java.lang.Integer" ->
                        integerIndex.insert((Integer) page.tuples.get(0).getValues().get(_columnName), pageName);
                case "java.lang.Double" ->
                        doubleIndex.insert((Double) page.tuples.get(0).getValues().get(_columnName), pageName);
                case "java.lang.String" ->
                        stringIndex.insert((String) page.tuples.get(0).getValues().get(_columnName), pageName);
            }
        }
    }
    public void insert(String key, String value) {
        stringIndex.insert(key,value);
    }
    public void insert(Integer key, String value) {
        integerIndex.insert(key,value);
    }
    public void insert(Double key, String value) {
        doubleIndex.insert(key,value);
    }
    public String search(String key) {
        return stringIndex.search(key);
    }
    public String search(Integer key) {
        return integerIndex.search(key);
    }
    public String search(Double key) {
        return doubleIndex.search(key);
    }
    public void delete(String key) {
        stringIndex.delete(key);
    }
    public void delete(Integer key) {
        integerIndex.delete(key);
    }
    public void delete(Double key) {
        doubleIndex.delete(key);
    }
}
