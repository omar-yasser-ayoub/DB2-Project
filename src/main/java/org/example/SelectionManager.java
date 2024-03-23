package org.example;

import java.io.Serializable;
import java.util.Vector;

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
}