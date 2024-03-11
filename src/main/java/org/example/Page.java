package org.example;

import java.io.*;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable {

    int pageNum;
    Vector<Tuple> tuples = new Vector<>();
    Table parentTable;
    int numOfRows;

    /**
     * Constructor for the Page class
     * @param parentTable The table that the page is a part of
     * @param pageNum The identifying number of the page
     */
    public Page(Table parentTable, int pageNum) throws DBAppException {
        this.parentTable = parentTable;
        this.pageNum = pageNum;

        Properties prop = new Properties();
        String fileName = "src/main/java/org/example/resources/DBApp.config";
        try (InputStream input = new FileInputStream(fileName)) {
            prop.load(input);
            this.numOfRows = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
        }
        catch (IOException e) {
            throw new DBAppException(e.getMessage());
        }
    }

    /**
     * Attempts to insert into the Page instance and returns whether the insertion was successful
     * @param tuple The tuple to be inserted into the page
     * @return true if the tuple was inserted successfully, false otherwise
     */
    public boolean insertIntoPage(Tuple tuple){
        if (tuples == null) {
            tuples = new Vector<>();
        }
        if (tuples.size() < numOfRows) {
            tuples.add(tuple);
            return true;
        }
        return false;
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
