package org.example;

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
        Tuple overflowTuple = insertTupleIntoPage(tuple, page);

        if (overflowTuple != null) {
            table.createPageInTable(overflowTuple, index);
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
            
            //if page is last page insert
            if (i == table.getPageNames().size() - 1) {
                return page;
            }

            //if tuple is greater than first tuple in page, insert into previous page
            if (clusteringKeyValue.compareTo(firstClusteringKeyValue) > 0) {
                page = FileManager.deserializePage(table.getPageNames().get(i - 1));
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
    public static Tuple insertTupleIntoPage(Tuple tuple, Page page) throws DBAppException {
        Table parentTable = page.getParentTable();
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
        //if page is greater than max size, return overflow tuple
        else {
            return tuple;
        }
        //if page became greater than max size after insertion, return overflow tuple
        if (tuples.size() > DBApp.maxRowCount){
            Tuple overflowTuple = tuples.remove(tuples.size() - 1);
            page.save();
            return overflowTuple;
        }
        //if page is still less than max size, return null because no overflow
        return null;
    }
}