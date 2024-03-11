package org.example;

import java.util.Vector;

public class Tuple implements java.io.Serializable {
    private Vector<Object> values;
    public Tuple() {
        values = new Vector<>();
    }
    public void insert(Object e) {
        try {
            values.add(e);
        } catch (NullPointerException ex) {
            System.out.println("Cannot insert null element: " + ex.getMessage());
        } catch (IllegalArgumentException ex) {
            System.out.println("Invalid argument: " + ex.getMessage());
        } catch (UnsupportedOperationException ex) {
            System.out.println("Operation not supported: " + ex.getMessage());
        } catch (ClassCastException ex) {
            System.out.println("Incompatible types: " + ex.getMessage());
        } catch (IndexOutOfBoundsException ex) {
            System.out.println("Index out of bounds: " + ex.getMessage());
        }
    }
    public void remove(int index) {
        if (index >= 0 && index < values.size()) {
            values.remove(index);
        } else {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for list of size " + values.size());
        }
    }
    public void replace(int index, Object value) {
        if (index >= 0 && index < values.size()) {
            values.remove(index);
            values.add(index, value);
        } else {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for list of size " + values.size());
        }
    }
    public String toString() {
        StringBuilder returnString = new StringBuilder();
        for (Object value : values) {
            returnString.append(String.valueOf(value));
        }
        return returnString.toString();
    }
}
