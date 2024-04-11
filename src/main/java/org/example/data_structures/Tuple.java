package org.example.data_structures;

import org.example.exceptions.DBAppException;

import java.util.Hashtable;

public class Tuple implements java.io.Serializable {
    private Hashtable<String, Object> values;
    public Tuple() {
        values = new Hashtable<>();
    }
    public Tuple(Hashtable<String, Object> values) {
        this.values = values;
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

    public int getPrimaryKeyValue() {
        return 1;
    }
}
