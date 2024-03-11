package org.example;

import java.util.Hashtable;

public class Tuple implements java.io.Serializable {
    private Hashtable<Object, Object> values;
    public Tuple() {
        values = new Hashtable<>();
    }
    public void insert(Object key, Object value) throws DBAppException {
        try {
            values.put(key, value);
        } catch (Exception ex) {
            throw new DBAppException(ex.getMessage());
        }
    }
    public void remove(Object key) throws DBAppException {
        try {
            values.remove(key);
        } catch (Exception ex) {
            throw new DBAppException(ex.getMessage());
        }
    }
    public void replace(Object key, Object value) throws DBAppException {
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
