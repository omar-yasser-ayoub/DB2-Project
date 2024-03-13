package org.example;

import java.io.*;
import java.util.Vector;

public class Page implements Serializable {

    int pageNum;
    Vector<Tuple> tuples;
    Table parentTable;

    /**
     * Constructor for the Page class
     * @param parentTable The table that the page is a part of
     * @param pageNum The identifying number of the page
     */
    public Page(Table parentTable, int pageNum){
        this.parentTable = parentTable;
        this.pageNum = pageNum;
        this.tuples = new Vector<>();
    }

    //TODO: Implement binary search
    /**
     * Attempts to insert into the Page instance
     * @param tuple The tuple to be inserted into the page
     * @return Overflowing tuple if the page is full
     */
    public Tuple insertIntoPage(Tuple tuple){
        String clusteringKey = parentTable.clusteringKey;
        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(clusteringKey);

        if (tuples.size() <= DBApp.maxRowCount) {
            if(tuples.isEmpty()){
                tuples.add(tuple);
                return null;
            }
            for (int i = 0; i < tuples.size(); i++) {
                Comparable<Object> currentClusteringKeyValue = (Comparable<Object>) tuples.get(i).getValues().get(clusteringKey);
                if (clusteringKeyValue.compareTo(currentClusteringKeyValue) < 0){
                    tuples.add(i, tuple);
                    break;
                }
                if (clusteringKeyValue.compareTo(currentClusteringKeyValue) > 0 && i == tuples.size()-1){
                    tuples.add(i+1, tuple);
                    break;
                }
            }
        }
        else {
            return tuple;
        }
        if (tuples.size() > DBApp.maxRowCount){
            return tuples.remove(tuples.size() - 1);
        }
        return null;
    }

    /**
     * Serializes the page object to the disk
     * @throws DBAppException If an error occurs during serialization
     */
    public void serializePage() throws DBAppException {
        createSerializedDirectory();
        String fileName = "data/serialized_pages/" + parentTable.tableName + pageNum + ".ser";
        try (FileOutputStream fileOut = new FileOutputStream(fileName); ObjectOutputStream objOut = new ObjectOutputStream(fileOut)) {
            objOut.writeObject(this);
        } catch (IOException e) {
            throw new DBAppException(e.getMessage());
        }
    }

    private static void createSerializedDirectory() throws DBAppException {
        String directoryPath = "data/serialized_pages";
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (!created) {
                throw new DBAppException("Failed to create directory: " + directoryPath);
            }
        }
    }

    public static Page deserializePage(String fileName) throws DBAppException {
        try (FileInputStream fileIn = new FileInputStream(fileName); ObjectInputStream objIn = new ObjectInputStream(fileIn)) {
            return  (Page) objIn.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public String toString() {
        if (tuples == null || tuples.isEmpty()) {
            return "Page is Empty";
        }
        StringBuilder returnString = new StringBuilder();
        for (Object tuple : tuples) {
            returnString.append(tuple.toString());
            returnString.append(",");
        }
        returnString.deleteCharAt(returnString.length() - 1);
        return returnString.toString();
    }
}
