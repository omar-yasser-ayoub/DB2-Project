package org.example.managers;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.example.data_structures.index.Index;
import org.example.exceptions.DBAppException;
import org.example.data_structures.SQLTerm;
import org.example.data_structures.Tuple;
import org.example.data_structures.Page;
import org.example.data_structures.Table;

import java.io.File;
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

    private static Vector<Tuple> indexSearchInTable(SQLTerm term, Index index, Table table) throws DBAppException {
        Vector<String> pageNames = new Vector<>();
        Vector<Tuple> finalList = new Vector<>();
        switch (term.getStrOperator()) {
            case "=":
                pageNames = index.getbTree().equalSearch((Comparable) term.getObjValue());
                break;
            case "!=":
                return linearSearchInTable(term, table);
            case ">":
                pageNames = index.getbTree().greaterThanSearch((Comparable) term.getObjValue());
                break;
            case ">=":
                pageNames = index.getbTree().greaterThanOrEqualSearch((Comparable) term.getObjValue());
                break;
            case "<":
                pageNames = index.getbTree().lessThanSearch((Comparable) term.getObjValue());
                break;
            case "<=":
                pageNames = index.getbTree().lessThanOrEqualSearch((Comparable) term.getObjValue());
                break;
            default:
                return null;
        }
        for (String pageName : pageNames) {
            Page page = FileManager.deserializePage(pageName);
            linearSearchInPage(term, finalList, page);
        }
        return finalList;
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
        //"value" attribute is the value we want to find, can be int, String or double
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
    }

    public static int compareObjects(Object obj1 , Object obj2){
        // ((Comparable) Object).compareTo((Comparable) otherObject)); lol
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

        computeSQLTerm(arrSQLTerms[0], table);
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
            return null;
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