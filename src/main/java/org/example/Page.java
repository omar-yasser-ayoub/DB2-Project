package org.example;

import java.util.Vector;

public class Page {

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
        return True;
    }

    private void serializePage(){}

    private void deserialziePage(){}

}
