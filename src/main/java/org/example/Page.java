package org.example;

import java.io.*;
import java.util.Vector;

public class Page implements Serializable {

    int pageNum;
    Vector<Tuple> tuples;
    Table parentTable;

    /**
     * Constructor for the Page class
     * @param parentTable The table that the page is a part of
     * @param pageNum The identifying number of the page
     */
    public Page(Table parentTable, int pageNum){
        this.parentTable = parentTable;
        this.pageNum = pageNum;
        this.tuples = new Vector<>();
    }

    public int getNumOfTuples(){
        return tuples.size();
    }
    //TODO: Implement binary search
    /**
     * Attempts to insert into the Page instance
     * @param tuple The tuple to be inserted into the page
     * @return Overflowing tuple if the page is full
     */
    public Tuple insertIntoPage(Tuple tuple) throws DBAppException {
        String clusteringKey = parentTable.clusteringKey;
        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(clusteringKey);

        if (tuples.size() <= DBApp.maxRowCount) {
            //if page is empty, insert into page
            if(tuples.isEmpty()){
                tuples.add(tuple);
                this.serializePage();
                return null;
            }
            for (int i = 0; i < tuples.size(); i++) {
                Comparable<Object> currentClusteringKeyValue = (Comparable<Object>) tuples.get(i).getValues().get(clusteringKey);
                //if tuple is less than current tuple, insert before current tuple
                if (clusteringKeyValue.compareTo(currentClusteringKeyValue) < 0){
                    tuples.add(i, tuple);
                    this.serializePage();
                    break;
                }
                //if tuple is greater than current tuple and is the last tuple, insert after current tuple
                if (clusteringKeyValue.compareTo(currentClusteringKeyValue) > 0 && i == tuples.size()-1){
                    tuples.add(i+1, tuple);
                    this.serializePage();
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
            this.serializePage();
            return overflowTuple;
        }
        //if page is still less than max size, return null because no overflow
        return null;
    }
    //TODO: Implement delete page from disk
    public int deleteFromPage(Tuple tuple) throws DBAppException {
        String clusteringKey = parentTable.clusteringKey;
        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(clusteringKey);

        for (int i = 0; i < tuples.size(); i++){
            Comparable<Object> currentClusteringKeyValue = (Comparable<Object>) tuples.get(i).getValues().get(clusteringKey);
            if (clusteringKeyValue.compareTo(currentClusteringKeyValue) == 0){
                tuples.remove(i);
                if(tuples.isEmpty()) {
                    //delete page from disk
                    return 0;                                    //page empty after deletion, call deletePage
                } else {
                    this.serializePage();
                    return 1;                                //deleted and shifted
                }
            }
        }
        return 2;                                                //not found
    }


    /**
     * Serializes the page object to the disk
     * @throws DBAppException If an error occurs during serialization
     */
    public void serializePage() throws DBAppException {
        createSerializedDirectory();
        String fileName = "data/serialized_pages/" + parentTable.tableName + pageNum + ".ser";
        try (FileOutputStream fileOut = new FileOutputStream(fileName); ObjectOutputStream objOut = new ObjectOutputStream(fileOut)) {
            objOut.writeObject(this);
        } catch (IOException e) {
            throw new DBAppException(e.getMessage());
        }
    }

    private static void createSerializedDirectory() throws DBAppException {
        String directoryPath = "data/serialized_pages";
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (!created) {
                throw new DBAppException("Failed to create directory: " + directoryPath);
            }
        }
    }

    public static Page deserializePage(String pageName) throws DBAppException {
        String fileName = "data/serialized_pages/" + pageName + ".ser";
        try (FileInputStream fileIn = new FileInputStream(fileName); ObjectInputStream objIn = new ObjectInputStream(fileIn)) {
            return  (Page) objIn.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public String toString() {
        if (tuples == null || tuples.isEmpty()) {
            return "Page is Empty";
        }
        StringBuilder returnString = new StringBuilder();
        for (Object tuple : tuples) {
            returnString.append(tuple.toString());
            returnString.append(",");
        }
        returnString.deleteCharAt(returnString.length() - 1);
        return returnString.toString();
    }

    public static boolean binarySearch(Page page, String key, Object value){
        //key is column name
        //"value" attribute is the value we wanna find , can be int,String or double
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
                return true;
           } else if (comparisonResult < 0) {
                // If our value is greater than current value, search in the right half
                low = mid + 1;
           } else {
                // If our value is less than current value, search in the left half
                high = mid - 1;
           }
        }
        return false;
    }

    public static int compareObjects(Object obj1 , Object obj2){
       if(  obj1 instanceof Integer && obj2 instanceof Integer){
            Integer currI = (Integer)(obj1);
            Integer valI = (Integer)(obj2);
            return currI.compareTo(valI);
       }
       else if( obj1 instanceof Double && obj2 instanceof Double){
           Double currD = (Double)(obj1);
           Double valD = (Double)(obj2);
           return currD.compareTo(valD) ;
       }
       else if(  obj1 instanceof String && obj2 instanceof String) {
           String currS = (String) (obj1);
           String valS = (String) (obj2);
           return currS.compareTo(valS);       //if first>second then positive
       }
       else{
           throw new IllegalArgumentException("Objects must be of type Integer, Double, or String");
       }
    }
}
