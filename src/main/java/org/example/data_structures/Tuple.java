package org.example.data_structures;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.example.exceptions.DBAppException;

import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import static org.example.DBApp.METADATA_DIR;

public class Tuple implements java.io.Serializable {
    private Hashtable<String, Object> values;
    private String primaryKeyColumnName;

    public Tuple() {
        values = new Hashtable<>();
    }
    public Tuple(Hashtable<String, Object> values) {
        this.values = values;
    }
    public Tuple(Hashtable<String, Object> values, String primaryKeyColumnName) {
        this.values = values;
        this.primaryKeyColumnName = primaryKeyColumnName;
    }
    public Tuple(String primaryKeyColumnName) {
        values = new Hashtable<>();
        this.primaryKeyColumnName = primaryKeyColumnName;
    }
    public Comparable getPrimaryKeyValue() {
        return (Comparable) values.get(primaryKeyColumnName);
    }
    public Hashtable<String, Object> getValues() {
        return values;
    }
    public void insert(String key, Object value) throws DBAppException {
        try {
            values.put(key, value);
        } catch (Exception ex) {
            throw new DBAppException(ex.getMessage());
        }
    }
    public void remove(String key) throws DBAppException {
        try {
            values.remove(key);
        } catch (Exception ex) {
            throw new DBAppException(ex.getMessage());
        }
    }
    public void replace(String key, Object value) throws DBAppException {
        try {
            values.remove(key);
            values.put(key, value);
        } catch (Exception ex) {
            throw new DBAppException(ex.getMessage());
        }
    }
    public String toString() {
        StringBuilder returnString = new StringBuilder();
        for (Object key : values.keySet()) {
            returnString.append(String.valueOf(this.values.get(key)));
            returnString.append(",");
        }
        returnString.deleteCharAt(returnString.length() - 1);
        return returnString.toString();
    }
}
