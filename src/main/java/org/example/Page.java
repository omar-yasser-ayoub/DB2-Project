package org.example;

import java.io.*;
import java.util.Vector;

public class Page implements Serializable {

    static int PAGE_COUNT = 0;
    Vector<Tuple> tuples;
    Table parentTable;
    int numOfRows;

    /**
     * Constructor for the Page class
     * @param parentTable The table that the page is a part of
     * @param numOfRows The number of rows that the page can hold
     */
    public Page(Table parentTable, int numOfRows) {
        this.parentTable = parentTable;
        this.numOfRows = numOfRows;
    }

    /**
     * Attempts to insert into the Page instance and returns whether the insertion was successful
     * @param tuple The tuple to be inserted into the page
     * @return True if the tuple was inserted successfully, false otherwise
     */
    public boolean insertIntoPage(Tuple tuple){
        return true;
    }

    /**
     * Serializes the page object to the disk
     * @throws DBAppException If an error occurs during serialization
     */
    public void serializePage() throws DBAppException {
        String directoryPath = "data/serialized_pages";
        String fileName = "data/serialized_pages/page" + PAGE_COUNT + ".ser";

        File directory = new File(directoryPath);
        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (!created) {
                throw new DBAppException("Failed to create directory: " + directoryPath);
            }
        }

        try {
            FileOutputStream fileOut = new FileOutputStream(fileName);
            ObjectOutputStream objOut = new ObjectOutputStream(fileOut);
            objOut.writeObject(this);

            objOut.flush();
            objOut.close();
            PAGE_COUNT++;
        } catch (IOException e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public static Page deserializePage(String FileName) throws DBAppException{

        try {
            FileInputStream FileIn = new FileInputStream(FileName);
            ObjectInputStream ObjIn = new ObjectInputStream(FileIn);
            Page p = (Page) ObjIn.readObject();
            ObjIn.close();
            return p;
        } catch (IOException e) {
            throw new DBAppException(e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new DBAppException(e.getMessage());
        }


    }

    public String toString() {
        StringBuilder returnString = new StringBuilder();
        for (Object tuple : tuples) {
            returnString.append(tuple.toString());
        }
        return returnString.toString();
    }
}
