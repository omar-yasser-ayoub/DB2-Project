package org.example.managers;

import org.example.data_structures.index.Index;
import org.example.exceptions.DBAppException;
import org.example.data_structures.Tuple;
import org.example.data_structures.Page;
import org.example.data_structures.Table;

import java.util.Vector;

public class DeletionManager{
    private DeletionManager() {
        throw new IllegalStateException("Utility class");
    }

    public static void deleteFromTable(Tuple tuple, Table table) throws DBAppException {
        table.isValidTuple(tuple);

        Comparable<Object> clusteringKeyValue = (Comparable<Object>) tuple.getValues().get(table.getClusteringKey());

        int pageIndex = SelectionManager.getIndexOfPageFromClusteringValue(clusteringKeyValue, table);
        if (pageIndex < 0) {
            System.out.println("Tuple not found in table");
            return;
        }
        Page page = FileManager.deserializePage(table.getPageNames().get(pageIndex));

        int tupleIndex = SelectionManager.getIndexOfTupleFromClusteringValue(clusteringKeyValue, page);
        if (tupleIndex < 0) {
            System.out.println("Tuple not found in table");
            return;
        }
        tuple = page.getTuples().get(tupleIndex);

        page.getTuples().remove(tupleIndex);
        updateIndexOnDeletion(tuple, table);

        if(page.getTuples().isEmpty()) {
            FileManager.deletePage(page.getPageName()); //delete from disk
            table.getPageNames().remove(pageIndex);

            table.save();
            return;
        }
        page.save();

    }

    protected static void updateIndexOnDeletion(Tuple tuple, Table parentTable) throws DBAppException {
        Vector<String> indexNames = parentTable.getIndices();
        if (indexNames != null) {
            for (String indexName : indexNames) {
                Index index = FileManager.deserializeIndex(indexName,parentTable );
                index.delete(tuple.getValues().get(index.getColumnName()));
                index.save();
            }
        }
    }
}