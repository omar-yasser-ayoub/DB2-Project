package org.example.managers;

import org.example.DBApp;
import org.example.exceptions.DBAppException;
import org.example.data_structures.Tuple;
import org.example.data_structures.Page;
import org.example.data_structures.Table;

import java.util.Vector;

public class InsertionManager{
    private InsertionManager() {
        throw new IllegalStateException("Utility class");
    }
    public static void insertTupleIntoTable(Tuple tuple, Table table) throws DBAppException {
        table.isValidTuple(tuple);

        if (table.getPageNames().isEmpty()) {
            table.createPageInTable();
        }

        Page page = getCorrectPageForInsertion(tuple, table);
        int index = table.getPageNames().indexOf(page.getPageName());
        Tuple overflowTuple = insertTupleIntoPage(tuple, table, page);

        while (overflowTuple != null) {
            Page nextPage = FileManager.deserializePage(table.getPageNames().get(index + 1));
            index++;
            Tuple nextOverflowTuple = insertTupleIntoPage(overflowTuple, table, nextPage);
            overflowTuple = nextOverflowTuple;
        }
    }

    /**
     * Finds the correct page to insert the tuple into so that the table is sorted by the clustering key.
     *
     * @param tuple The tuple to be inserted into the page
     * @param table The table that the tuple is being inserted into
     * @return An object array containing the index of the page and the tuple that overflowed
     */
    private static Page getCorrectPageForInsertion(Tuple tuple, Table table) throws DBAppException {
        Vector<String> pageNames = table.getPageNames();
        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(table.getClusteringKey());

        for (String pageName : pageNames) {
            Page page = FileManager.deserializePage(pageName);
            Vector<Tuple> tuples = page.getTuples();
            int i = pageNames.indexOf(pageName);

            //if page is empty, insert into page
            if (tuples.isEmpty()) {
                return page;
            }

            Tuple firstTuple = tuples.get(0);
            Comparable<Object> firstClusteringKeyValue = (Comparable<Object>) firstTuple.getValues().get(table.getClusteringKey());

            //if tuple is smaller than first tuple in page, insert into previous page
            if (clusteringKeyValue.compareTo(firstClusteringKeyValue) < 0) {
                if (i == 0) {
                    return page;
                }
                page = FileManager.deserializePage(table.getPageNames().get(i - 1));
                return page;
            }

            //if page is last page insert
            if (i == table.getPageNames().size() - 1) {
                return page;
            }


        }
        return null;
    }

    /**
     * Attempts to insert into the Page instance
     * @param tuple The tuple to be inserted into the page
     * @return Overflowing tuple if the page is full
     */
    public static Tuple insertTupleIntoPage(Tuple tuple, Table parentTable, Page page) throws DBAppException {
        String clusteringKey = parentTable.getClusteringKey();
        Vector<Tuple> tuples = page.getTuples();
        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(clusteringKey);

        if (tuples.size() <= DBApp.maxRowCount) {
            //if page is empty, insert into page
            if(tuples.isEmpty()){
                tuples.add(tuple);
                page.save();
                return null;
            }
            for (int i = 0; i < tuples.size(); i++) {
                Comparable<Object> currentClusteringKeyValue = (Comparable<Object>) tuples.get(i).getValues().get(clusteringKey);
                //if tuple is less than current tuple, insert before current tuple
                if (clusteringKeyValue.compareTo(currentClusteringKeyValue) < 0){
                    tuples.add(i, tuple);
                    page.save();
                    break;
                }
                //if tuple is greater than current tuple and is the last tuple, insert after current tuple
                if (clusteringKeyValue.compareTo(currentClusteringKeyValue) > 0 && i == tuples.size()-1){
                    tuples.add(i+1, tuple);
                    page.save();
                    break;
                }
            }
        }
        //if page is greater than max size, create overflow page and insert tuple into it directly
        else {
            int index = parentTable.getPageNames().indexOf(page.getPageName());
            parentTable.createPageInTable(tuple,index);
            return null;
        }
        //if page overflowed after insertion, return overflow tuple to shift down
        if (tuples.size() > DBApp.maxRowCount){
            //if page is last page, create new page
            if(page.getPageNum() == parentTable.getPageCount() - 1){
                parentTable.createPageInTable();
            }
            Tuple overflowTuple = tuples.remove(tuples.size() - 1);
            page.save();
            return overflowTuple;
        }
        //if page is still less than max size, return null because no overflow
        return null;
    }
}