package org.example.managers;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.example.data_structures.Page;
import org.example.data_structures.Table;
import org.example.data_structures.Tuple;
import org.example.exceptions.DBAppException;

import java.io.*;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import static org.example.DBApp.METADATA_DIR;

public class UpdateManager implements Serializable {
    private UpdateManager() {
        throw new IllegalStateException("Utility class");
    }

    public static void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue) throws DBAppException {
        String[] nameAndType = isValidUpdate(strTableName, strClusteringKeyValue, htblColNameValue);

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

    private static String[] isValidUpdate(String strTableName, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValues) throws DBAppException {
        try {
            // create a reader
            CSVReader reader = new CSVReader(new FileReader(METADATA_DIR + "/metadata.csv"));
            String[] line = reader.readNext();

            String clusteringKeyName = "";
            String clusteringKeyType = "";
            Vector<String> colNames = new Vector<>();

            while ((line = reader.readNext()) != null) {
                if(line[0].equals(strTableName)) {
                    // gets the name and type of the clustering key column
                    if(line[3].equals("True")) {
                        clusteringKeyName = line[1];
                        clusteringKeyType = line[2];
                    }
                    else { // checks if all other columns are in hashtable and if datatypes match
                        if(!htblColNameValues.containsKey(line[1])) {
                            throw new DBAppException("Missing columns in hashtable");
                        }
                        else {
                            String type = line[2];
                            Object val = htblColNameValues.get(line[1]);
                            if((type.equals("java.lang.Integer") && !(val instanceof Integer))
                                || (type.equals("java.lang.Double") && !(val instanceof Double))
                                || (type.equals("java.lang.String") && !(val instanceof String))) {
                                throw new DBAppException("Column value doesn't match datatype");
                            }
                            colNames.add(line[1]);
                        }
                    }
                }
            }

            // checks if all columns in hashtable are in the table
            Enumeration<String> keys = htblColNameValues.keys();
            while (keys.hasMoreElements()) {
                String key = keys.nextElement();
                if(!colNames.contains(key)) {
                    throw new DBAppException("Hashtable contains extra column(s)");
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
        } catch (CsvValidationException | IOException e) {
            throw new RuntimeException(e);
        } catch (NumberFormatException e) {
            throw new DBAppException("Clustering key value is of incorrect datatype");
        }
    }
}
