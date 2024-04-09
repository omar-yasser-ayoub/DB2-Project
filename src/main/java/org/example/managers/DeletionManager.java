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

        int pageIndex = SelectionManager.getIndexOfPage(clusteringKeyValue, table);
        Page page = FileManager.deserializePage(table.getPageNames().get(pageIndex));

        int tupleIndex = SelectionManager.getIndexOfTuple(clusteringKeyValue, page);
        tuple = page.getTuples().get(tupleIndex);

        page.getTuples().remove(tupleIndex);
        updateIndexOnDeletion(tuple, table);

        if(page.getTuples().isEmpty()) {
            FileManager.deleteFile(page.getPageName()); //delete from disk
            table.getPageNames().remove(pageIndex);

            table.save();
            return;
        }
        page.save();

    }

    protected static void updateIndexOnDeletion(Tuple tuple, Table parentTable) throws DBAppException {
        Vector<Index> indices = parentTable.getIndices();
        if (indices != null) {
            for (Index index : indices) {
                index.delete(tuple.getValues().get(index.getColumnName()));
                index.save();
            }
        }
    }
}