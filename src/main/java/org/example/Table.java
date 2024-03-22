package org.example;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static org.example.Page.deserializePage;

public class Table implements Serializable {
    private String tableName;
    private Hashtable<String,String> colNameType;
    private String clusteringKey;
    private Hashtable<String, String> indicesColNameType;
    private Vector<String> pageNames;
    private int pageCount;
    private String keyType;
    public Object index;

    public Table(String tableName, String clusteringKey, Hashtable<String,String> colNameType) throws IOException, CsvValidationException, DBAppException {
        this.tableName = tableName;
        this.colNameType = colNameType;
        this.clusteringKey = clusteringKey;
        this.pageNames = new Vector<>();
        this.pageCount = 0;

        writeMetadata(tableName, clusteringKey, colNameType);
    }

    private static void writeMetadata(String tableName, String clusteringKey, Hashtable<String, String> colNameType) throws IOException {
        CSVWriter writer = DBApp.writer;
        Enumeration<String> columns = colNameType.keys();
        while (columns.hasMoreElements()){
            String column = columns.nextElement();
            String[] info = {tableName,
                    column,
                    colNameType.get(column),
                    Objects.equals(clusteringKey, column) ? "True" : "False",
                    "null",
                    "null"};

            writer.writeNext(info);
        }
        writer.flush();
    }



    /**
     * Creates a new page and adds it to the table
     * @return The newly created page
     */
    public Page createPage() throws DBAppException {
        Page newPage = new Page(this, pageCount);
        newPage.serializePage();
        String pageName = tableName + pageCount;
        pageNames.add(pageName);
        pageCount++;
        return newPage;
    }
    /**
     * Creates a new page with a tuple, and adds it to the table at the specified index
     * @param tuple The tuple to be inserted into the page
     * @param index The index at which the page is to be inserted
     * @return The newly created page
     */
    public Page createPage(Tuple tuple, int index) throws DBAppException {
        Page newPage = new Page(this, pageCount);
        newPage.serializePage();
        String pageName = tableName + pageCount;
        pageNames.add(index + 1, pageName);
        newPage.insertIntoPage(tuple);
        return newPage;
    }

    public void insertIntoTable(Tuple tuple) throws DBAppException {
        isValidTuple(tuple);

        if(pageNames.isEmpty()){
            createPage();
        }

        Object[] overflowTupleIndex = insertIntoCorrectPage(tuple);
        if (overflowTupleIndex[0] != null){
            createPage((Tuple)overflowTupleIndex[0], (int) overflowTupleIndex[1]);
        }
    }

    //TODO: Implement binary search

    /**
     * Finds the correct page to insert the tuple into so that the table is sorted by the clustering key.
     * @param tuple The tuple to be inserted into the page
     * @return An object array containing the index of the page and the tuple that overflowed
     */
    private Object[] insertIntoCorrectPage(Tuple tuple) throws DBAppException {
        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(clusteringKey);
        Object[] overflowTupleIndex = new Object[2];

        for (String pageName : pageNames){
            Page page = deserializePage(pageName);
            int i = pageNames.indexOf(pageName);
            //if page is empty, insert into page
            if (page.tuples.isEmpty()){
                overflowTupleIndex[1] = i;
                overflowTupleIndex[0] = page.insertIntoPage(tuple);
                return overflowTupleIndex;
            }

            Tuple firstTuple = page.tuples.get(0);
            Comparable<Object> firstClusteringKeyValue = (Comparable<Object>) firstTuple.getValues().get(clusteringKey);
            //if page is last page insert
            if(i == pageNames.size()-1){
                overflowTupleIndex[1] = i;
                overflowTupleIndex[0] = page.insertIntoPage(tuple);
                return overflowTupleIndex;
            }

            //if tuple is greater than first tuple in page, insert into previous page
            if (clusteringKeyValue.compareTo(firstClusteringKeyValue) > 0){
                page = deserializePage(pageNames.get(i-1));
                overflowTupleIndex[1] = i;
                overflowTupleIndex[0] = page.insertIntoPage(tuple);
                return overflowTupleIndex;
            }
        }
        return overflowTupleIndex;
    }

    private boolean isValidTuple(Tuple tuple) throws DBAppException {
        Hashtable<String, Object> values = tuple.getValues();
        for (String key : values.keySet()){
            //check if key is in colNameType
            if (!colNameType.containsKey(key)){
                throw new DBAppException("Key not found in table");
            }

            //check if value is of the correct type
            try {
                Class<?> expectedClass = Class.forName(colNameType.get(key));
                expectedClass.cast(values.get(key));
            } catch (ClassNotFoundException | ClassCastException ex) {
                throw new DBAppException("Key is not of the correct type");
            }

            //check if clustering key is unique
            if (key.equals(clusteringKey) && (!tupleHasNoDuplicateClusteringKey(key, values.get(key)))){
                throw new DBAppException("Clustering key is not unique");
            }

        }
        return true;
    }

    private static int compareObjects(Object obj1 , Object obj2){
        if(  obj1 instanceof String && obj2 instanceof String){
            String currS = (String)(obj1);
            String valS = (String)(obj2);

            return currS.compareTo(valS);       //if first>second then positive


        }
        if(  obj1 instanceof Integer && obj2 instanceof Integer){
            Integer currI = (Integer)(obj1);
            Integer valI = (Integer)(obj2);

            return currI.compareTo(valI);
        }
        if( obj1 instanceof Double && obj2 instanceof Double){
            Double currD = (Double)(obj1);
            Double valD = (Double)(obj2);

            return currD.compareTo(valD)   ;
        }
        else
            return 0;
    }
    //TODO: Implement binary search
    private boolean tupleHasNoDuplicateClusteringKey(String key, Object value) throws DBAppException {
        int numberOfPages = pageNames.size();
        int startPageNum = 0;

        while (startPageNum < numberOfPages) {
            //get current page number
            int currPageNum = startPageNum + (numberOfPages - startPageNum) / 2;

            //get Page
            String pageName = pageNames.get(currPageNum);
            Page page = deserializePage(pageName);

            //is page empty
            if (page.tuples.isEmpty()) {
                startPageNum = currPageNum + 1; // Move to the next page
                continue;
            }

            int startTupleNum = 0;
            int numberOfTuples = page.getNumOfTuples();

            // Binary search within the current page
            int low = startTupleNum;
            int high = numberOfTuples - 1;
            while (low <= high) {
                int mid = low + (high - low) / 2;
                Tuple tuple = page.tuples.get(mid);

                // Compare the key value with the target value
                int comparisonResult = compareObjects(tuple.getValues().get(key), value);

                if (comparisonResult == 0) {
                    // Key-value pair found, return false
                    return false;
                } else if (comparisonResult < 0) {
                    // If our value is greater than current value, search in the right half
                    low = mid + 1;
                } else {
                    // If our value is less than current value, search in the left half
                    high = mid - 1;
                }
            }

            // Move to the next page
            startPageNum = currPageNum + 1;
        }
        return true;
    }

    public void deleteFromTable(Tuple tuple)throws DBAppException {
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

        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(clusteringKey);
        int deletionResult = -1;

        if (pageNames.isEmpty()) {
            throw new DBAppException("Table is already empty");
        }else{
            for (String pageName : pageNames){
                int i = pageNames.indexOf(pageName);
                Page page = deserializePage(pageName);
                Tuple firstTuple = page.tuples.get(0); //FIRST TUPLE OF CURRENT PAGE
                Comparable<Object> firstClusteringKeyValue = (Comparable<Object>) firstTuple.getValues().get(clusteringKey); //VALUE OF THAT TUPLE
                if (clusteringKeyValue.compareTo(firstClusteringKeyValue) < 0){
                    Page prevPage = deserializePage(pageNames.get(i-1));
                    deletionResult = prevPage.deleteFromPage(tuple);
                    switch(deletionResult) {
                        case 0:
                            pageNames.remove(i-1);
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

    public Vector<Tuple> linearSearch(SQLTerm Term) throws DBAppException {
        Vector<Tuple> finalList = new Vector<Tuple>();
        for (String pageName : pageNames) {
            Page page = deserializePage(pageName);
            if (page.tuples.isEmpty()){
                continue;
            }
            for (Tuple tuple : page.tuples) {
                switch (Term._strOperator) {
                    case ">":
                        if (Term._objValue instanceof String) {
                            int value = ((String) tuple.getValues().get(Term._strColumnName)).compareTo((String) Term._objValue);
                            if (value > 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Integer) {
                            int value = ((Integer) tuple.getValues().get(Term._strColumnName)).compareTo((Integer) Term._objValue);
                            if (value > 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Double) {
                            int value = ((Double) tuple.getValues().get(Term._strColumnName)).compareTo((Double) Term._objValue);
                            if (value > 0) {
                                finalList.add(tuple);
                            }
                        }
                        break;
                    case ">=":
                        if (Term._objValue instanceof String) {
                            int value = ((String) tuple.getValues().get(Term._strColumnName)).compareTo((String) Term._objValue);
                            if (value > 0 || value == 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Integer) {
                            int value = ((Integer) tuple.getValues().get(Term._strColumnName)).compareTo((Integer) Term._objValue);
                            if (value > 0 || value == 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Double) {
                            int value = ((Double) tuple.getValues().get(Term._strColumnName)).compareTo((Double) Term._objValue);
                            if (value > 0 || value == 0) {
                                finalList.add(tuple);
                            }
                        }
                        break;
                    case "<":
                        if (Term._objValue instanceof String) {
                            int value = ((String) tuple.getValues().get(Term._strColumnName)).compareTo((String) Term._objValue);
                            if (value < 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Integer) {
                            int value = ((Integer) tuple.getValues().get(Term._strColumnName)).compareTo((Integer) Term._objValue);
                            if (value < 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Double) {
                            int value = ((Double) tuple.getValues().get(Term._strColumnName)).compareTo((Double) Term._objValue);
                            if (value < 0) {
                                finalList.add(tuple);
                            }
                        }
                        break;
                    case "<=":
                        if (Term._objValue instanceof String) {
                            int value = ((String) tuple.getValues().get(Term._strColumnName)).compareTo((String) Term._objValue);
                            if (value < 0 || value == 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Integer) {
                            int value = ((Integer) tuple.getValues().get(Term._strColumnName)).compareTo((Integer) Term._objValue);
                            if (value < 0 || value == 0) {
                                finalList.add(tuple);
                            }
                        }
                        if (Term._objValue instanceof Double) {
                            int value = ((Double) tuple.getValues().get(Term._strColumnName)).compareTo((Double) Term._objValue);
                            if (value < 0 || value == 0) {
                                finalList.add(tuple);
                            }
                        }
                        break;
                    case "=":
                        if (tuple.getValues().get(Term._strColumnName).equals(Term._objValue)) {
                            finalList.add(tuple);
                        }
                        break;
                    case "!=":
                        if (!tuple.getValues().get(Term._strColumnName).equals(Term._objValue)) {
                            finalList.add(tuple);
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        return finalList;
    }
}