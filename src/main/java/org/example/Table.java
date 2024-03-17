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
    String tableName;
    Hashtable<String,String> colNameType;
    String clusteringKey;
    Hashtable<String, String> indicesColNameType;
    Vector<String> pageNames;
    int pageCount;
    String keyType;
    public Object index;

    public Table(String tableName, String clusteringKey, Hashtable<String,String> colNameType) throws IOException, CsvValidationException, DBAppException {
        CSVReader reader = new CSVReader(new FileReader("src/main/java/org/example/resources/metadata.csv"));
        String[] line = reader.readNext();

        while((line = reader.readNext()) != null){
            if(tableName.equals(line[0])){
                throw new DBAppException("Table already exists");
            }
        }

        this.tableName = tableName;
        this.colNameType = colNameType;
        this.clusteringKey = clusteringKey;
        this.pageNames = new Vector<>();
        this.pageCount = 0;

        CSVWriter writer = new CSVWriter(DBApp.outputFile, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.RFC4180_LINE_END);
        Enumeration<String> e = colNameType.keys();
        while (e.hasMoreElements()){
            String key = e.nextElement();
            String[] info = {tableName,
                    key,
                    colNameType.get(key),
                    clusteringKey==key ? "True" : "False",
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
        //for (String pageName : pageNames)
        //key is column name, value is value of column for this record
        int startTupleNum = 0;
        int numberOfTuples=0;

        int numberOfPages = pageNames.size();
        int startPageNum = 0;

        while(startPageNum <= numberOfPages){
            //get current page number
            int currPageNum = startPageNum + (numberOfPages -1) /2;

            //get Page
            String pageName = pageNames.get(currPageNum);
            Page page = deserializePage(pageName);
            numberOfTuples = page.getNumOfTuples();

            //is page empty
            if (page.tuples.isEmpty()){
                continue;
            }

            //loop on page to check if i found sth similar
            while(startTupleNum <= numberOfTuples){
                //get current tuple
                int currTupleNum = startTupleNum + (numberOfTuples - 1) / 2 ;
                Tuple tuple = page.tuples.get(currTupleNum);

                //if tuple is our target
                if (tuple.getValues().get(key).equals(value)){
                    return false;
                }
                //if our value is greater than current value, ignore left half
                if(  compareObjects(tuple.getValues().get(key) , value) < 0){
                   startTupleNum = currTupleNum + 1;
                }
                else{
                    startTupleNum = currTupleNum - 1;
                }

            }

            //if i didnt find sth similar, binary search on page
            //check

        }
        return true;
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

    public void createIndex(String _keyType, String column) throws DBAppException {
        switch (_keyType) {
            case "Integer":
                index = new BTree<Integer, String>();
                keyType = "Integer";
                break;
            case "Double":
                index = new BTree<Double, String>();
                keyType = "Double";
                break;
            case "String":
                index = new BTree<String, String>();
                keyType = "String";
                break;
            default:
                throw new DBAppException("Invalid key type: " + _keyType);
        }

        for (String pageName : pageNames) {
            Page page = deserializePage(pageName);
            if (page.tuples.isEmpty()) {
                continue;
            }

            switch (_keyType) {
                case "Integer":
                    ((BTree<Integer,String>) index).insert((Integer) page.tuples.get(0).getValues().get(column), pageName);
                    break;
                case "Double":
                    ((BTree<Double,String>) index).insert((Double) page.tuples.get(0).getValues().get(column), pageName);
                    break;
                case "String":
                    ((BTree<String,String>) index).insert((String) page.tuples.get(0).getValues().get(column), pageName);
                    break;
            }
        }
    }
    public void updateIndex(){
        //update index
    }
}