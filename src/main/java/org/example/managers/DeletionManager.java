package org.example.managers;

import org.example.data_structures.index.Index;
import org.example.exceptions.DBAppException;
import org.example.data_structures.Tuple;
import org.example.data_structures.Page;
import org.example.data_structures.Table;

import java.util.Vector;

public class DeletionManager{
    private DeletionManager() {
        throw new IllegalStateException("Utility class");
    }

    public static void deleteFromTable(Tuple tuple, Table table) throws DBAppException {

        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(table.getClusteringKey());

        if (table.getPageNames().isEmpty()) {
            throw new DBAppException("Table is already empty");
        } else {
            int startPageNum = 0;
            int numberOfPages = table.getPageNames().size();
            while (startPageNum < numberOfPages) {

                //get current page number
                int currPageNum = startPageNum + (numberOfPages - startPageNum) / 2;

                //get Page
                String pageName = table.getPageNames().get(currPageNum);
                Page page = FileManager.deserializePage(pageName);

                //First and Last Tuple of Current Page
                Tuple firstTuple = page.getTuples().get(0);
                int numberOfTuplesInPage = page.getTuples().size();
                Tuple lastTuple = page.getTuples().get(numberOfTuplesInPage -1);

                //Value of these Tuples
                Comparable<Object> firstClusteringKeyValue = (Comparable<Object>) firstTuple.getValues().get(table.getClusteringKey());
                Comparable<Object> LastClusteringKeyValue = (Comparable<Object>) lastTuple.getValues().get(table.getClusteringKey());

                if (clusteringKeyValue.compareTo(firstClusteringKeyValue) >= 0 && clusteringKeyValue.compareTo(LastClusteringKeyValue) <= 0) {
                    deleteFromPage(tuple, table, page);
                    return;
                }
                else if(clusteringKeyValue.compareTo(firstClusteringKeyValue) < 0){
                    numberOfPages = currPageNum - 1;
                }
                else{
                    startPageNum = currPageNum + 1;
                }

            }
            throw new DBAppException("Tuple Page not Found");
            /*startPageNum = currPageNum + 1;
                    Page prevPage = FileManager.deserializePage(table.getPageNames().get(currPageNum - 1));*/
        }
    }
    public static void deleteFromPage(Tuple tuple, Table table, Page page) throws DBAppException {
        String clusteringKey = page.getParentTable().getClusteringKey();

        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(clusteringKey);

        int startTupleNum = 0;
        int numberOfTuples = page.getTuples().size();

        // Binary search within the current page
        int low = startTupleNum;
        int high = numberOfTuples - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;

            Comparable<Object> currentClusteringKeyValue = (Comparable<Object>) page.getTuples().get(mid).getValues().get(clusteringKey);

            // Compare the key value with the target value


            if (clusteringKeyValue.compareTo(currentClusteringKeyValue) == 0){
                // Key-value pair found, return false
                page.getTuples().remove(mid);
                updateIndexOnDeletion(tuple, table);

                if(page.getTuples().isEmpty()) {
                    FileManager.deleteFile(page.getPageName()); //delete from disk
                    table.getPageNames().remove(mid);
                    table.save();
                    return;

                } else {
                    page.save();
                    return;
                }
            } else if (clusteringKeyValue.compareTo(currentClusteringKeyValue) < 0) {
                // If our value is less than current value, search in the left half
                high = mid - 1;
            } else {
                // If our value is greater than current value, search in the right half
                low = mid + 1;
            }
        }
        throw new DBAppException("Tuple not Found"); //tuple not found
    }

    private static void updateIndexOnDeletion(Tuple tuple, Table parentTable) throws DBAppException {
        Vector<Index> indices = parentTable.getIndices();
        for (Index index : indices) {
            index.delete(tuple.getValues().get(index.getColumnName()));
            index.save();
        }
    }
}