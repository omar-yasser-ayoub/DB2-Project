package org.example.managers;

import org.example.exceptions.DBAppException;
import org.example.data_structures.Tuple;
import org.example.data_structures.Page;
import org.example.data_structures.Table;

public class DeletionManager{
    private DeletionManager() {
        throw new IllegalStateException("Utility class");
    }

    public static void deleteFromTable(Tuple tuple, Table table) throws DBAppException {
//        int numberOfPages = pageNames.size();
//        int startPageNum = 0;
//
//        while (startPageNum < numberOfPages) {
//            //get current page number
//            int currPageNum = startPageNum + (numberOfPages - startPageNum) / 2;
//
//            //get Page
//            String pageName = pageNames.get(currPageNum);
//            Page page = deserializePage(pageName);
//
//            //is page empty
//            if (page.tuples.isEmpty()) {
//                startPageNum = currPageNum + 1; // Move to the next page
//                continue;
//            }
//
//            int startTupleNum = 0;
//            int numberOfTuples = page.getNumOfTuples();
//
//            // Binary search within the current page
//            int low = startTupleNum;
//            int high = numberOfTuples - 1;
//            while (low <= high) {
//                int mid = low + (high - low) / 2;
//                Tuple currentTuple = page.tuples.get(mid);
//
//                // Compare the key value with the target value
//                int comparisonResult = compareObjects(currentTuple.getValues().get(clusteringKey), tuple.getValues().get(clusteringKey));
//
//                if (comparisonResult == 0) {
//                    int deletionResult = page.deleteFromPage(tuple);
//                    switch (deletionResult) {
//                        case 0 -> pageNames.remove(currPageNum);
//                        case 1 -> {
//                            return;
//                        }
//                        default -> throw new DBAppException("Tuple not Found");
//                    }
//
//                } else if (comparisonResult < 0) {
//                    // If our value is greater than current value, search in the right half
//                    low = mid + 1;
//                } else {
//                    // If our value is less than current value, search in the left half
//                    high = mid - 1;
//                }
//            }
//
//            // Move to the next page
//            startPageNum = currPageNum + 1;
//        }
//        return;
        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(table.getClusteringKey());
        int deletionResult = -1;

        if (table.getPageNames().isEmpty()) {
            throw new DBAppException("Table is already empty");
        } else {
            for (String pageName : table.getPageNames()) {
                int i = table.getPageNames().indexOf(pageName);
                Page page = FileManager.deserializePage(pageName);
                Tuple firstTuple = page.getTuples().get(0); //FIRST TUPLE OF CURRENT PAGE
                Comparable<Object> firstClusteringKeyValue = (Comparable<Object>) firstTuple.getValues().get(table.getClusteringKey()); //VALUE OF THAT TUPLE
                if (clusteringKeyValue.compareTo(firstClusteringKeyValue) < 0) {
                    Page prevPage = FileManager.deserializePage(table.getPageNames().get(i - 1));
                    deletionResult = deleteFromPage(tuple, prevPage);
                    switch (deletionResult) {
                        case 0:
                            table.getPageNames().remove(i - 1);
                            break;
                        case 1:
                            return;
                        default:
                            throw new DBAppException("Tuple not Found");
                    }
                }
            }
        }
    }

    //TODO: Implement delete page from disk
    public static int deleteFromPage(Tuple tuple, Page page) throws DBAppException {
        String clusteringKey = page.getParentTable().getClusteringKey();
        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(clusteringKey);

        for (int i = 0; i < page.getTuples().size(); i++){
            Comparable<Object> currentClusteringKeyValue = (Comparable<Object>) page.getTuples().get(i).getValues().get(clusteringKey);
            if (clusteringKeyValue.compareTo(currentClusteringKeyValue) == 0){
                page.getTuples().remove(i);
                if(page.getTuples().isEmpty()) {
                    //delete page from disk
                    return 0;                                    //page empty after deletion, call deletePage
                } else {
                    page.save();
                    return 1;                                //deleted and shifted
                }
            }
        }
        return 2;                                                //not found
    }
}