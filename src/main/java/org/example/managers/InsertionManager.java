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
        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(table.getClusteringKey());

        int low = 0;
        int high = table.getPageNames().size();
        int lastfound = 0;

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

            Tuple firstTuple = page.getTuples().get(0);
            Tuple lastTuple = page.getTuples().get(page.getTuples().size() -1);

            Comparable<Object> firstClusteringKeyValue = (Comparable<Object>) firstTuple.getValues().get(table.getClusteringKey());
            Comparable<Object> lastClusteringKeyValue = (Comparable<Object>) lastTuple.getValues().get(table.getClusteringKey());

            if (clusteringKeyValue.compareTo(firstClusteringKeyValue) >= 0 && clusteringKeyValue.compareTo(lastClusteringKeyValue) <= 0) {
                return page;
            }

            else if(clusteringKeyValue.compareTo(firstClusteringKeyValue) < 0){
                //if tuple is smaller and page is first page insert
                if (mid == 0) {
                    return page;
                }
                lastfound = mid;
                high = mid - 1;
            }

            else{
                //if tuple is greater and page is last page insert
                if (mid == table.getPageNames().size() - 1) {
                    return page;
                }
                lastfound = mid;
                low = mid + 1;
            }
        }

        String pageName = table.getPageNames().get(lastfound);
        return FileManager.deserializePage(table.getPageNames().get(lastfound));
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

            int i = getCorrectIndexOfTupleinPage(page,tuple,clusteringKey);
            tuples.add(i, tuple);
            page.save();
            updateIndexOnInsertion(tuple, parentTable, page);
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
            DeletionManager.updateIndexOnDeletion(overflowTuple, parentTable);
            page.save();
            return overflowTuple;
        }
        //if page is still less than max size, return null because no overflow
        return null;
    }
    private static int getCorrectIndexOfTupleinPage(Page page, Tuple tuple, String clusteringKey){
        Vector<Tuple> tuples = page.getTuples();
        Comparator<Tuple> c = Comparator.comparing(u -> ((Comparable<Object>) u.getValues().get(clusteringKey)));
        int i = Collections.binarySearch(tuples, tuple, c); //returns index of tuple if found, otherwise returns -(insertion point) - 1
        if(i<0){
            i++;
            i*=-1;
        }
        return i;
    }
   /**
     * Updates the index of the parent table after a tuple is inserted
     * @param tuple The tuple that was inserted
     * @param parentTable The table that the tuple was inserted into
     * @param page The page that the tuple was inserted into
     */
    private static void updateIndexOnInsertion(Tuple tuple, Table parentTable, Page page) throws DBAppException {
        Vector<Index> indices = parentTable.getIndices();
        if (indices != null) {
            for (Index index : indices) {
                index.insert(tuple.getValues().get(index.getColumnName()), page.getPageName());
                index.save();
            }
        }
    }
}