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
import java.util.*;

import static org.example.DBApp.METADATA_DIR;

public class SelectionManager implements Serializable {
    private SelectionManager() {
        throw new IllegalStateException("Utility class");
    }

    private static Vector<Tuple> indexSearchInTable(SQLTerm term, Index index, Table table) throws DBAppException {
        Vector<String> pageNames;
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
            //TODO: Binary Search?
            linearSearchInPage(term, finalList, page);
        }
        return finalList;
    }

    private static Vector<Tuple> binarySearchInTable(SQLTerm term,  Table table) throws DBAppException {
        Vector<Tuple> finalList = new Vector<>();
        Comparable<Object> value = (Comparable<Object>) term.getObjValue();
        int pageIndex = getIndexOfPageFromClusteringValue(value, table);
        //TODO: What if tuple not found?
        Page page = FileManager.deserializePage(table.getPageNames().get(pageIndex));
        Vector<Tuple> tuples = page.getTuples();
        int tupleIndex;

        switch (term.getStrOperator()) {
            case "=" -> {
                tupleIndex = getIndexOfTupleFromClusteringValue(value, page);
                finalList.add(tuples.get(tupleIndex));
            }
            case "!=" -> {
                return linearSearchInTable(term, table);
            }
            case ">" -> {
                tupleIndex = getIndexOfTupleFromClusteringValue(value, page);
                for (int i = tupleIndex + 1; i < tuples.size(); i++) {
                    finalList.add(tuples.get(i));
                }
                for (int i = pageIndex + 1; i < table.getPageNames().size(); i++) {
                    Page nextPage = FileManager.deserializePage(table.getPageNames().get(i));
                    finalList.addAll(nextPage.getTuples());
                }
            }
            case ">=" -> {
                tupleIndex = getIndexOfTupleFromClusteringValue(value, page);
                for (int i = tupleIndex; i < tuples.size(); i++) {
                    finalList.add(tuples.get(i));
                }
                for (int i = pageIndex + 1; i < table.getPageNames().size(); i++) {
                    Page nextPage = FileManager.deserializePage(table.getPageNames().get(i));
                    finalList.addAll(nextPage.getTuples());
                }
            }
            case "<" -> {
                tupleIndex = getIndexOfTupleFromClusteringValue(value, page);
                for (int i = 0; i < tupleIndex; i++) {
                    finalList.add(tuples.get(i));
                }
                for (int i = 0; i < pageIndex; i++) {
                    Page nextPage = FileManager.deserializePage(table.getPageNames().get(i));
                    finalList.addAll(nextPage.getTuples());
                }
            }
            case "<=" -> {
                tupleIndex = getIndexOfTupleFromClusteringValue(value, page);
                for (int i = 0; i <= tupleIndex; i++) {
                    finalList.add(tuples.get(i));
                }
                for (int i = 0; i < pageIndex; i++) {
                    Page nextPage = FileManager.deserializePage(table.getPageNames().get(i));
                    finalList.addAll(nextPage.getTuples());
                }
            }
            default -> {
                return null;
            }
        }
        return finalList;
    }

    /**
     * Returns the index of the page that contains the tuple with the given clustering value
     * @param value The clustering value of the tuple
     * @param table The table to search in
     * @return The index of the page that contains the tuple with the given clustering value
     * @throws DBAppException If the tuple is not found
     */
    protected static int getIndexOfPageFromClusteringValue(Comparable<Object> value, Table table) throws DBAppException {
        int low = 0;
        int high = table.getPageNames().size() - 1;

        while (low <= high) {

            int mid = low + (high - low) / 2;

            //get Page
            String pageName = table.getPageNames().get(mid);
            Tuple minTuple = FileManager.deserializePageMin(pageName);
            Tuple maxTuple = FileManager.deserializePageMax(pageName);

            Comparable<Object> firstClusteringKeyValue = (Comparable<Object>) minTuple.getValues().get(table.getClusteringKey());
            Comparable<Object> lastClusteringKeyValue = (Comparable<Object>) maxTuple.getValues().get(table.getClusteringKey());

            if (value.compareTo(firstClusteringKeyValue) >= 0 && value.compareTo(lastClusteringKeyValue) <= 0) {
                return mid;
            } else if (value.compareTo(firstClusteringKeyValue) < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        throw new DBAppException("Tuple not found while binary searching table");
    }
    /**
     * Returns the index of the tuple with the given clustering value in the given page
     * @param value The clustering value of the tuple
     * @param page The page to search in
     * @return The index of the tuple with the given clustering value in the given page
     * @throws DBAppException If the tuple is not found
     */
    protected static int getIndexOfTupleFromClusteringValue(Comparable<Object> value, Page page) throws DBAppException {
        Vector<Tuple> tuples = page.getTuples();
        Table parentTable = page.getParentTable();
        String clusteringKey = parentTable.getClusteringKey();
        int low = 0;
        int high = tuples.size() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            Tuple tuple = tuples.get(mid);
            Comparable<Object> columnValue = (Comparable<Object>)tuple.getValues().get(clusteringKey);
            int comparison = columnValue.compareTo(value);
            if (comparison == 0) {
                return mid;
            } else if (comparison < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        throw new DBAppException("Tuple not found while binary searching page");
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

    public static Iterator<Tuple> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException{
        isValidSQLTerm(arrSQLTerms, strarrOperators);
        String tableName = arrSQLTerms[0].getStrTableName();
        Table table = FileManager.deserializeTable(tableName);
        Vector<Vector<Tuple>> totalTuples = new Vector<>();
        for (SQLTerm arrSQLTerm : arrSQLTerms) {
            totalTuples.add(computeSQLTerm(arrSQLTerm, table));
        }

        Vector<Tuple> result = evalQuery(totalTuples, strarrOperators);

        return result != null ? result.iterator() : null;
    }

    private static Vector<Tuple> computeSQLTerm(SQLTerm sqlTerm, Table table) throws DBAppException {
        if (!table.getIndices().isEmpty()) {
            for (String indexName : table.getIndices()) {
                Index index = FileManager.deserializeIndex(indexName);
                if (index.getColumnName().equals(sqlTerm.getStrColumnName())) {
                    return indexSearchInTable(sqlTerm, index, table);
                }
            }
        }
        if (table.getClusteringKey().equals(sqlTerm.getStrColumnName())) {
            return binarySearchInTable(sqlTerm, table);
        }
        else {
            return linearSearchInTable(sqlTerm, table);
        }
    }

    private static void isValidSQLTerm(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
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
                CSVReader reader = new CSVReader(new FileReader(METADATA_DIR + "/metadata.csv"));
                String[] line = reader.readNext();

                // exception message stuff
                String exceptionMsg = "exceptionMsg";
                boolean colInTable = false;
                boolean tableExists = false;
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

    private static int getPrecedence(String op) {
        // 1 = lowest precedence
        switch (op) {
            case "OR" -> {
                return 1;
            }
            case "XOR" -> {
                return 2;
            }
            case "AND" -> {
                return 3;
            }
        }
        return -1;
    }

    private static Vector<Tuple> logicalAnd(Vector<Tuple> tuples1, Vector<Tuple> tuples2) {
        Vector<Hashtable<String, Object>> htv = new Vector<>();
        for(Tuple t : tuples2) {
            htv.add(t.getValues());
        }

        Vector<Tuple> result = new Vector<>();
        for (Tuple tuple : tuples1) {
            if (htv.contains(tuple.getValues())) {
                result.add(tuple);
            }
        }
        return result;
    }

    private static Vector<Tuple> logicalOr(Vector<Tuple> tuples1, Vector<Tuple> tuples2) {
        Vector<Hashtable<String, Object>> htv = new Vector<>();
        for(Tuple t : tuples1) {
            htv.add(t.getValues());
        }

        Vector<Tuple> result = new Vector<>(tuples1);
        for (Tuple tuple : tuples2) {
            if (!htv.contains(tuple.getValues())) {
                htv.add(tuple.getValues());
                result.add(tuple);
            }
        }
        return result;
    }

    private static Vector<Tuple> logicalXor(Vector<Tuple> tuples1, Vector<Tuple> tuples2) {
        Vector<Hashtable<String, Object>> htv1 = new Vector<>();
        for(Tuple t : tuples1) {
            htv1.add(t.getValues());
        }
        Vector<Hashtable<String, Object>> htv2 = new Vector<>();
        for(Tuple t : tuples2) {
            htv2.add(t.getValues());
        }

        Vector<Tuple> result = new Vector<>();
        for (Tuple tuple1 : tuples1) {
            if (!htv2.contains(tuple1.getValues())) {
                result.add(tuple1);
            }
        }
        for (Tuple tuple2 : tuples2) {
            if (!htv1.contains(tuple2.getValues())) {
                result.add(tuple2);
            }
        }
        return result;
    }

    public static Vector<Tuple> evalQuery(Vector<Vector<Tuple>> allTuples, String[] strarrOperators) {
        int j = 1;
        Stack<String> opStack = new Stack<>();
        Stack<Vector<Tuple>> tupleStack = new Stack<>();
        tupleStack.push(allTuples.get(0));

        for (String strOperator : strarrOperators) {
            tupleStack.push(allTuples.get(j));
            Vector<Tuple> tmp = null;
            while (!opStack.isEmpty() && getPrecedence(strOperator) < getPrecedence(opStack.peek())) {
                if(tmp == null){
                    tmp = tupleStack.pop();
                }
                switch (opStack.pop()) {
                    case "AND" -> tupleStack.push(logicalAnd(tupleStack.pop(), tupleStack.pop()));
                    case "XOR" -> tupleStack.push(logicalXor(tupleStack.pop(), tupleStack.pop()));
                    case "OR" -> tupleStack.push(logicalOr(tupleStack.pop(), tupleStack.pop()));
                }
            }

            if(tmp != null){
                tupleStack.push(tmp);
            }
            opStack.push(strOperator);
            j++;
        }

        while(!opStack.isEmpty()) {
            String strOperator = opStack.pop();
            switch (strOperator) {
                case "AND" -> tupleStack.push(logicalAnd(tupleStack.pop(), tupleStack.pop()));
                case "XOR" -> tupleStack.push(logicalXor(tupleStack.pop(), tupleStack.pop()));
                case "OR" -> tupleStack.push(logicalOr(tupleStack.pop(), tupleStack.pop()));
            }
        }

        return !tupleStack.isEmpty() ? tupleStack.pop() : null;
    }

}