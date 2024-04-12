package org.example.managers;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.example.data_structures.Page;
import org.example.data_structures.Table;
import org.example.data_structures.Tuple;
import org.example.exceptions.DBAppException;

import java.io.*;
import java.util.Hashtable;

import static org.example.DBApp.METADATA_DIR;

public class UpdateManager implements Serializable {
    private UpdateManager() {
        throw new IllegalStateException("Utility class");
    }

    public static void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue) throws DBAppException {
        String[] nameAndType = isValidUpdate(strTableName, strClusteringKeyValue);

        Comparable value = null;
        switch (nameAndType[1]) {
            case "java.lang.Integer":
                value = Integer.valueOf(Integer.parseInt(strClusteringKeyValue));
                break;
            case "java.lang.Double":
                value = Double.valueOf(Double.parseDouble(strClusteringKeyValue));
                break;
            case "java.lang.String":
                value = strClusteringKeyValue;
                break;
        }

        htblColNameValue.put(nameAndType[0], value);

        Table table = FileManager.deserializeTable(strTableName);
        int pageIndex = SelectionManager.getIndexOfPageFromClusteringValue(value, table);
        Page page = FileManager.deserializePage(table.getPageNames().get(pageIndex));
        int tupleIndex = SelectionManager.getIndexOfTupleFromClusteringValue(value, page);
        page.getTuples().set(tupleIndex, new Tuple(htblColNameValue));

        page.save();
        table.save();
    }

    private static String[] isValidUpdate(String strTableName, String strClusteringKeyValue) throws DBAppException {
        try {
            // create a reader
            CSVReader reader = new CSVReader(new FileReader(METADATA_DIR + "/metadata.csv"));
            String[] line = reader.readNext();

            String clusteringKeyName = "";
            String clusteringKeyType = "";

            // gets the name and type of the clustering key column
            while ((line = reader.readNext()) != null) {
                if(line[0].equals(strTableName)) {
                    if(line[3].equals("True")) {
                        clusteringKeyName = line[1];
                        clusteringKeyType = line[2];
                        break;
                    }
                }
            }

            // if value was never assigned, table doesn't exist
            if(clusteringKeyName.equals(""))
                throw new DBAppException("Table does not exist");

            // throws an exception if the value doesn't match the clustering key type
            switch (clusteringKeyType) {
                case "java.lang.Integer":
                    Integer.parseInt(strClusteringKeyValue); break;
                case "java.lang.Double":
                    Double.parseDouble(strClusteringKeyValue); break;
                case "java.lang.String":
                    break;
            }

            return(new String[]{clusteringKeyName, clusteringKeyType});
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
