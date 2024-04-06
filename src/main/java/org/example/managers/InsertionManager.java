package org.example.managers;

import org.example.DBApp;
import org.example.data_structures.index.Index;
import org.example.exceptions.DBAppException;
import org.example.data_structures.Tuple;
import org.example.data_structures.Page;
import org.example.data_structures.Table;

import java.util.Collections;
import java.util.Comparator;
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
        int i = table.getPageNames().indexOf(page.getPageName());
        Tuple overflowTuple = insertTupleIntoPage(tuple, table, page);

        while (overflowTuple != null) {
            Page nextPage = FileManager.deserializePage(table.getPageNames().get(i + 1));
            i++;
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


        int low = 0;
        int high = table.getPageNames().size();

        while (low <= high) {

            int mid = low + (high - low) / 2;

            //get Page
            String pageName = table.getPageNames().get(mid);
            Page page = FileManager.deserializePage(pageName);
            Vector<Tuple> tuples = page.getTuples();


            //if page is empty, insert into page
            if (tuples.isEmpty()) {
                return page;
            }

            //First and Last Tuple of Current Page
            Tuple firstTuple = page.getTuples().get(0);
            int numberOfTuplesInPage = page.getTuples().size();
            Tuple lastTuple = page.getTuples().get(numberOfTuplesInPage -1);

            //Value of these Tuples
            Comparable<Object> firstClusteringKeyValue = (Comparable<Object>) firstTuple.getValues().get(table.getClusteringKey());
            Comparable<Object> lastClusteringKeyValue = (Comparable<Object>) lastTuple.getValues().get(table.getClusteringKey());

            //if tuple is smaller than first tuple in page, insert into previous page
            if (clusteringKeyValue.compareTo(firstClusteringKeyValue) >= 0 && clusteringKeyValue.compareTo(lastClusteringKeyValue) <= 0) {

                return page;
            }
            else if(clusteringKeyValue.compareTo(firstClusteringKeyValue) < 0){
                if ( mid == 0) {
                    return page;
                }
                high = mid - 1;
            }
            else{
                low = mid + 1;
            }

            //if page is last page insert
            if (mid == table.getPageNames().size() - 1) {
                return page;
            }


        }
        return null;
        /*for (String pageName : pageNames) {

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
        return null;*/
    }
    public static int tupleFoundInPage(Page page, Tuple tuple, String clusteringKey){
        Vector<Tuple> tuples = page.getTuples();
        Comparator<Tuple> c = (u1, u2) -> SelectionManager.compareObjects(u1.getValues().get(clusteringKey), u2.getValues().get(clusteringKey));
        return Collections.binarySearch(tuples, tuple, c);
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
                updateIndexOnInsertion(tuple, parentTable, page);
                return null;
            }
            //[1,3,6,8,9] insert 5--> -3 (return value +1 then *-1)

            int i = tupleFoundInPage(page,tuple,clusteringKey);
            if(i<0){
                i++;
                i*=-1;
            }
            tuples.add(i, tuple);
            page.save();
            updateIndexOnInsertion(tuple, parentTable, page);

            //TODO: Remove commented out code after testing
            /*int low = 0;
            int high = parentTable.getPageNames().size();
            while (low <= high) {

                //current tuple's index and is value
                int mid = low + (high - low) / 2;
                Comparable<Object> currentClusteringKeyValue = (Comparable<Object>) tuples.get(mid).getValues().get(clusteringKey);

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
            }*/
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

    private static void updateIndexOnInsertion(Tuple tuple, Table parentTable, Page page) throws DBAppException {
        Vector<Index> indices = parentTable.getIndices();
        for (Index index : indices) {
            index.insert(tuple.getValues().get(index.getColumnName()), page.getPageName());
            index.save();
        }
    }
}