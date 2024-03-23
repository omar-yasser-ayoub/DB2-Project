package org.example.managers;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
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

    public Vector<Tuple> linearSearchInTable(SQLTerm term, Table table) throws DBAppException {
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

    public static Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[]  strarrOperators) throws DBAppException{
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
        for (String strOperator : strarrOperators ) {
            if (!(strOperator.equals("AND") || strOperator.equals("OR") || strOperator.equals("XOR")))
                throw new DBAppException("Illegal operator");
        }
        return null;
    }
}