package org.example;

import java.io.*;

import static org.example.Page.deserializePage;

public class Index implements Serializable {



    String _indexType;
    String _columnName;
    Table _parentTable;

    public Index(Table parentTable, String indexType, String columnName) {
        this._indexType = indexType;
        this._parentTable = parentTable;
        this._columnName = columnName;
    }

    public void serializeIndex() throws DBAppException {
        createSerializedDirectory();
        String fileName = "data/serialized_indices/" + _parentTable.tableName + _columnName + ".ser";
        try (FileOutputStream fileOut = new FileOutputStream(fileName); ObjectOutputStream objOut = new ObjectOutputStream(fileOut)) {
            objOut.writeObject(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createSerializedDirectory() throws DBAppException {
        String directoryPath = "data/serialized_indices";
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (!created) {
                throw new DBAppException("Failed to create directory: " + directoryPath);
            }
        }
    }

    public static Index deserializeIndex(String indexName) throws DBAppException {
        String fileName = "data/serialized_indices/" + indexName + ".ser";
        try (FileInputStream fileIn = new FileInputStream(fileName); ObjectInputStream objIn = new ObjectInputStream(fileIn)) {
            return  (Index) objIn.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new DBAppException(e.getMessage());
        }
    }

//    public void createIndex(String _keyType, String column) throws DBAppException {
//        switch (_keyType) {
//            case "Integer":
//                index = new BTree<Integer, String>();
//                keyType = "Integer";
//                break;
//            case "Double":
//                index = new BTree<Double, String>();
//                keyType = "Double";
//                break;
//            case "String":
//                index = new BTree<String, String>();
//                keyType = "String";
//                break;
//            default:
//                throw new DBAppException("Invalid key type: " + _keyType);
//        }
//
//        for (String pageName : pageNames) {
//            Page page = deserializePage(pageName);
//            if (page.tuples.isEmpty()) {
//                continue;
//            }
//
//            switch (_keyType) {
//                case "Integer":
//                    ((BTree<Integer,String>) index).insert((Integer) page.tuples.get(0).getValues().get(column), pageName);
//                    break;
//                case "Double":
//                    ((BTree<Double,String>) index).insert((Double) page.tuples.get(0).getValues().get(column), pageName);
//                    break;
//                case "String":
//                    ((BTree<String,String>) index).insert((String) page.tuples.get(0).getValues().get(column), pageName);
//                    break;
//            }
//        }
//    }
}
