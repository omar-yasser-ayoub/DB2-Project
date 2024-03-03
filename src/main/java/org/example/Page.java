package org.example;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Vector;

public class Page {

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
        String fileName = "page" + PAGE_COUNT + ".ser";
        try {
            FileOutputStream fileOut = new FileOutputStream(fileName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        } catch (IOException e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public void deserializePage(){}

    public String toString() {
        StringBuilder returnString = new StringBuilder();
        for (Object tuple : tuples) {
            returnString.append(tuple.toString());
        }
        return returnString.toString();
    }
}
