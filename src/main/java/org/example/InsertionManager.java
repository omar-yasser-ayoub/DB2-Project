package org.example;

import java.io.Serializable;
import java.util.Vector;

public class InsertionManager implements Serializable {

    public static void insertIntoTable(Tuple tuple, Table table) throws DBAppException {
        table.isValidTuple(tuple);

        if (table.getPageNames().isEmpty()) {
            table.createPage();
        }

        Object[] overflowTupleIndex = insertIntoCorrectPage(tuple, table);
        if (overflowTupleIndex[0] != null) {
            table.createPage((Tuple) overflowTupleIndex[0], (int) overflowTupleIndex[1]);
        }
    }

    /**
     * Finds the correct page to insert the tuple into so that the table is sorted by the clustering key.
     *
     * @param tuple The tuple to be inserted into the page
     * @param table The table that the tuple is being inserted into
     * @return An object array containing the index of the page and the tuple that overflowed
     */
    private static Object[] insertIntoCorrectPage(Tuple tuple, Table table) throws DBAppException {
        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(table.getClusteringKey());
        Object[] overflowTupleIndex = new Object[2];

        for (String pageName : table.getPageNames()) {
            Page page = Page.deserializePage(pageName);
            int i = table.getPageNames().indexOf(pageName);
            //if page is empty, insert into page
            if (page.tuples.isEmpty()) {
                overflowTupleIndex[1] = i;
                overflowTupleIndex[0] = insertIntoPage(tuple, page);
                return overflowTupleIndex;
            }

            Tuple firstTuple = page.tuples.get(0);
            Comparable<Object> firstClusteringKeyValue = (Comparable<Object>) firstTuple.getValues().get(table.getClusteringKey());
            //if page is last page insert
            if (i == table.getPageNames().size() - 1) {
                overflowTupleIndex[1] = i;
                overflowTupleIndex[0] = insertIntoPage(tuple, page);
                return overflowTupleIndex;
            }

            //if tuple is greater than first tuple in page, insert into previous page
            if (clusteringKeyValue.compareTo(firstClusteringKeyValue) > 0) {
                page = Page.deserializePage(table.getPageNames().get(i - 1));
                overflowTupleIndex[1] = i;
                overflowTupleIndex[0] = insertIntoPage(tuple, page);
                return overflowTupleIndex;
            }
        }
        return overflowTupleIndex;
    }

    /**
     * Attempts to insert into the Page instance
     * @param tuple The tuple to be inserted into the page
     * @return Overflowing tuple if the page is full
     */
    public static Tuple insertIntoPage(Tuple tuple, Page page) throws DBAppException {
        Table parentTable = page.parentTable;
        Vector<Tuple> tuples = page.tuples;
        String clusteringKey = parentTable.clusteringKey;
        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(clusteringKey);

        if (tuples.size() <= DBApp.maxRowCount) {
            //if page is empty, insert into page
            if(tuples.isEmpty()){
                tuples.add(tuple);
                page.serializePage();
                return null;
            }
            for (int i = 0; i < tuples.size(); i++) {
                Comparable<Object> currentClusteringKeyValue = (Comparable<Object>) tuples.get(i).getValues().get(clusteringKey);
                //if tuple is less than current tuple, insert before current tuple
                if (clusteringKeyValue.compareTo(currentClusteringKeyValue) < 0){
                    tuples.add(i, tuple);
                    page.serializePage();
                    break;
                }
                //if tuple is greater than current tuple and is the last tuple, insert after current tuple
                if (clusteringKeyValue.compareTo(currentClusteringKeyValue) > 0 && i == tuples.size()-1){
                    tuples.add(i+1, tuple);
                    page.serializePage();
                    break;
                }
            }
        }
        //if page is greater than max size, return overflow tuple
        else {
            return tuple;
        }
        //if page became greater than max size after insertion, return overflow tuple
        if (tuples.size() > DBApp.maxRowCount){
            Tuple overflowTuple = tuples.remove(tuples.size() - 1);
            page.serializePage();
            return overflowTuple;
        }
        //if page is still less than max size, return null because no overflow
        return null;
    }
}