package org.example.managers;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.example.data_structures.index.Index;
import org.example.exceptions.DBAppException;
import org.example.data_structures.SQLTerm;
import org.example.data_structures.Tuple;
import org.example.data_structures.Page;
import org.example.data_structures.Table;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;

import static org.example.DBApp.METADATA_DIR;

public class SelectionManager implements Serializable {
    private SelectionManager() {
        throw new IllegalStateException("Utility class");
    }

    private static Vector<Tuple> indexSearchInTable(SQLTerm term, Index index, Table table) {
        switch (term.getStrOperator()) {
            case "=":
                return index.getbTree().equalSearch(term.getObjValue());
            case "!=":
                return index.getbTree().notEqualSearch(term.getObjValue());
            case ">":
                return index.getbTree().greaterThanSearch(term.getObjValue());
            case ">=":
                return index.getbTree().greaterThanOrEqualSearch(term.getObjValue());
            case "<":
                return index.getbTree().lessThanSearch(term.getObjValue());
            case "<=":
                return index.getbTree().lessThanOrEqualSearch(term.getObjValue());
            default:
                return null;
        }

    }
    public static Vector<Tuple> linearSearchInTable(SQLTerm term, Table table) throws DBAppException {
        Vector<Tuple> finalList = new Vector<>();
        for (String pageName : table.getPageNames()) {
            Page page = FileManager.deserializePage(pageName);
            if (page.getTuples().isEmpty()) {
                continue;
            }
            linearSearchInPage(term, finalList, page);
        }
        return finalList;
    }

    private static void linearSearchInPage(SQLTerm term, Vector<Tuple> finalList, Page page) {
        for (Tuple tuple : page.getTuples()) {
            String columnName = term.getStrColumnName();
            String columnValue = tuple.getValues().get(columnName).toString();
            String termValue = term.getObjValue().toString();
            int comparison = columnValue.compareTo(termValue);

            if (comparison > 0 && term.getStrOperator().equals(">") ||
                comparison >= 0 && term.getStrOperator().equals(">=") ||
                comparison < 0 && term.getStrOperator().equals("<") ||
                comparison <= 0 && term.getStrOperator().equals("<=") ||
                comparison == 0 && term.getStrOperator().equals("=") ||
                comparison != 0 && term.getStrOperator().equals("!=")) {
                finalList.add(tuple);
            }
        }
    }

    public static boolean binarySearchInPage(Page page, String key, Object value){
        //key is column name
        //"value" attribute is the value we want to find , can be int,String or double
        int startTupleNum = 0;
        int numberOfTuples = page.getTuples().size();

        // Binary search within the current page
        int low = startTupleNum;
        int high = numberOfTuples - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            Tuple tuple = page.getTuples().get(mid);

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

//        public boolean tupleHasNoDuplicateClusteringKey(String key, Object value) throws DBAppException {
//            //value here is the value itself: int, float, double
//            int low = 0;
//            int high = pageNames.size() -1;
//            int numberOfTuplesInPage;
//            boolean keyFound = false;
//
//            while (low <= high) {
//
//                //get current page number
//                int mid = low + (high - low) / 2;
//
//                //get Page
//                String pageName = pageNames.get(mid);
//                Page page = deserializePage(pageName);
//                numberOfTuplesInPage = page.getNumOfTuples();
//                Tuple min = page.tuples.get(0);
//                Tuple max = page.tuples.get(numberOfTuplesInPage -1);
//
//                //is page empty
//                if (page.tuples.isEmpty()) {
//                    // Assuming if page is empty then go left
//                    high = mid-1;
//                    continue;
//
//                }
//                //1 tuple in page and theyre similar
//                if(numberOfTuplesInPage ==1 && Page.compareObjects(value,min.getValues().get(key)) == 0){
//                    System.out.println("Duplicate found and number of tuples in page was 1");
//                    return false;
//                }
//                //one tuple in page and theyre not similar
//                if(numberOfTuplesInPage ==1 && Page.compareObjects(value,min.getValues().get(key)) != 0){
//                    System.out.println("Duplicate not found and number of tuples in page was 1");
//                    return true;
//                }
//                //Search in a page
//                if( Page.compareObjects(value,min.getValues().get(key)) >= 0 &&
//                        Page.compareObjects(max.getValues().get(key),value) >= 0){
//                    //value is in between 0 and last record
//                    keyFound = Page.binarySearch(page, key, value); //if found , true was returned
//
//                    if(keyFound == true){
//                        System.out.println("Duplicate found");
//                        return false;
//                    }
//                }
//                else if(Page.compareObjects(value,min.getValues().get(key)) < 0){
//                    //look in left side of pages
//                    high = mid-1;
//                }
//                else{
//                    //look in right side of pages
//                    high = mid+1;
//                }
//                if(pageNames.size()==1){
//                    break;
//                }
//
//            }
//            System.out.println("Duplicate not found");
//            return true;
//        }
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

    public static Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[]  strarrOperators) throws DBAppException{
        isValid(arrSQLTerms, strarrOperators);
        String tableName = arrSQLTerms[0].getStrTableName();
        Table table = FileManager.deserializeTable(tableName);

        computeSQLTerm(arrSQLTerms, table);
        return null;
    }

    private static Vector<Tuple> computeSQLTerm(SQLTerm sqlTerm, Table table) throws DBAppException {
        if (table.getIndices().size() != 0) {
            for (Index index : table.getIndices()) {
                if (index.getColumnName().equals(sqlTerm.getStrColumnName())) {
                    return indexSearchInTable(sqlTerm, index, table);
                }
            }
        }
        if (table.getClusteringKey().equals(sqlTerm.getStrColumnName())) {
            //binary search
        }
        else {
            return linearSearchInTable(sqlTerm, table);
        }
    }

    private static void isValid(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        if (arrSQLTerms.length == 0) {
            throw new DBAppException("Empty SQL Terms Array");
        }
        if (arrSQLTerms.length != strarrOperators.length + 1) {
            throw new DBAppException("Incompatible Array Lengths (arrSQLTerms should have a length bigger than strarrOperators by 1)");
        }
        String tableName = arrSQLTerms[0].getStrTableName();

        for (SQLTerm arrSQLTerm : arrSQLTerms) {
            if(!arrSQLTerm.getStrTableName().equals(tableName)) {
                throw new DBAppException("SQL Terms on different tables are not allowed");
            }
            // get datatype of value
            String type = "";
            if (arrSQLTerm.getObjValue() instanceof String) {
                type = "java.lang.String";
            } else if (arrSQLTerm.getObjValue() instanceof Integer) {
                type = "java.lang.Integer";
            } else if (arrSQLTerm.getObjValue() instanceof Double) {
                type = "java.lang.Double";
            }

            try {
                // create a reader
                CSVReader reader = new CSVReader(new FileReader(METADATA_DIR));
                String[] line = reader.readNext();

                // exception message stuff
                String exceptionMsg = "exceptionMsg";
                Boolean colInTable = false;
                Boolean tableExists = false;
                boolean flag = true;

                while ((line = reader.readNext()) != null) {
                    if (arrSQLTerm.getStrTableName().equals(line[0]) && arrSQLTerm.getStrColumnName().equals(line[1])) {
                        exceptionMsg = "Object datatype doesn't match column datatype";
                        colInTable = true;
                        tableExists = true;
                    }
                    if (arrSQLTerm.getStrTableName().equals(line[0]) && !arrSQLTerm.getStrColumnName().equals(line[1]) && !colInTable) {
                        exceptionMsg = "Column does not exist in table";
                        tableExists = true;
                    }
                    if (!tableExists)
                        exceptionMsg = "Table does not exist";

                    // if table exists and contains columns, and the datatype matches the column datatype, assign the values
                    if (arrSQLTerm.getStrTableName().equals(line[0]) && arrSQLTerm.getStrColumnName().equals(line[1]) && type.equals(line[2])) {
                        flag = false;
                        break;
                    }
                }
                // if values were never assigned, that means condition was never met and exception should be thrown
                if (flag)
                    throw new DBAppException(exceptionMsg);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (CsvValidationException e) {
                e.printStackTrace();
            }
            if (!(arrSQLTerm.getStrOperator().equals(">") || arrSQLTerm.getStrOperator().equals(">=") || arrSQLTerm.getStrOperator().equals("<") || arrSQLTerm.getStrOperator().equals("<=") || arrSQLTerm.getStrOperator().equals("!=") || arrSQLTerm.getStrOperator().equals("=")))
                throw new DBAppException("Illegal operator");
        }
        for (String strOperator : strarrOperators) {
            if (!(strOperator.equals("AND") || strOperator.equals("OR") || strOperator.equals("XOR")))
                throw new DBAppException("Illegal operator");
        }
    }
}