package org.example;

import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static org.example.Page.deserializePage;

public class Table implements Serializable {
    String tableName;
    Hashtable<String,String> colNameType;
    String clusteringKey;
    Hashtable<String, String> indicesColNameType;
    Vector<String> pageNames;
    int pageCount;
    String keyType;
    public Object index;

    public Table(String tableName, String clusteringKey, Hashtable<String,String> colNameType) throws IOException, CsvValidationException, DBAppException {
        this.tableName = tableName;
        this.colNameType = colNameType;
        this.clusteringKey = clusteringKey;
        this.pageNames = new Vector<>();
        this.pageCount = 0;
        writeMetadata();
    }

    public String getTableName() {
        return tableName;
    }

    public Hashtable<String, String> getColNameType() {
        return colNameType;
    }

    public String getClusteringKey() {
        return clusteringKey;
    }

    public Hashtable<String, String> getIndicesColNameType() {
        return indicesColNameType;
    }

    public Vector<String> getPageNames() {
        return pageNames;
    }

    public int getPageCount() {
        return pageCount;
    }

    public String getKeyType() {
        return keyType;
    }

    public Object getIndex() {
        return index;
    }

    //** Writes the metadata of the table to the metadata file
    private void writeMetadata() throws IOException {
        CSVWriter writer = DBApp.metadataWriter;
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
            writer.flush();
        }
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
        InsertionManager.insertIntoPage(tuple, newPage);
        return newPage;
    }

    public boolean isValidTuple(Tuple tuple) throws DBAppException {
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