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

    public int getPageNum() {
        return pageNum;
    }

    public Vector<Tuple> getTuples() {
        return tuples;
    }

    public Table getParentTable() {
        return parentTable;
    }

    public String getPageName() {
        return parentTable.tableName + pageNum;
    }

    //TODO: Implement delete page from disk
    public int deleteFromPage(Tuple tuple) throws DBAppException {
        String clusteringKey = parentTable.clusteringKey;
        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(clusteringKey);

        for (int i = 0; i < tuples.size(); i++){
            Comparable<Object> currentClusteringKeyValue = (Comparable<Object>) tuples.get(i).getValues().get(clusteringKey);
            if (clusteringKeyValue.compareTo(currentClusteringKeyValue) == 0){
                tuples.remove(i);
                if(tuples.isEmpty()) {
                    //delete page from disk
                    return 0;                                    //page empty after deletion, call deletePage
                } else {
                    this.serializePage();
                    return 1;                                //deleted and shifted
                }
            }
        }
        return 2;                                                //not found
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

    public static Page deserializePage(String pageName) throws DBAppException {
        String fileName = "data/serialized_pages/" + pageName + ".ser";
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
