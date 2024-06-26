package org.example.managers;

import org.example.data_structures.Tuple;
import org.example.exceptions.DBAppException;
import org.example.data_structures.Page;
import org.example.data_structures.Table;
import org.example.data_structures.index.Index;

import java.io.*;

public class FileManager implements Serializable {

    public static final String SERIALIZED_PAGES_PATH = "data/serialized_pages";
    public static final String SERIALIZED_PAGES_MINMAX_PATH = "data/serialized_pages_minmax";
    public static final String SERIALIZED_INDICES_PATH = "data/serialized_indices";
    public static final String SERIALIZED_TABLES_PATH = "data/serialized_tables";

    private FileManager() {
        throw new IllegalStateException("Utility class");
    }

    public static void createDirectory(String dirPath) throws DBAppException {
        File directory = new File(dirPath);
        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (!created) {
                throw new DBAppException("Failed to create directory: " + dirPath);
            }
        }
    }

    private static void serialize(Serializable object, String fileName) throws DBAppException {
        try (FileOutputStream fileOut = new FileOutputStream(fileName); ObjectOutputStream objOut = new ObjectOutputStream(fileOut)) {
            objOut.writeObject(object);
        } catch (IOException e) {
            throw new DBAppException(e.getMessage());
        }
    }

    private static Object deserialize(String fileName) throws DBAppException {
        try (FileInputStream fileIn = new FileInputStream(fileName); ObjectInputStream objIn = new ObjectInputStream(fileIn)) {
            return objIn.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new DBAppException("Table does not exist");
        }
    }

    public static void serializePage(Page page) throws DBAppException {
        createDirectory(SERIALIZED_PAGES_PATH);
        String fileName = SERIALIZED_PAGES_PATH + "/" + page.getPageName() + ".ser";
        serialize(page, fileName);
    }

    public static Page deserializePage(String pageName) throws DBAppException {
        String fileName = SERIALIZED_PAGES_PATH + "/" + pageName + ".ser";
        return (Page)deserialize(fileName);
    }

    public static void serializeIndex(Index index) throws DBAppException {
        createDirectory(SERIALIZED_INDICES_PATH);
        String fileName = SERIALIZED_INDICES_PATH  + "/" + index.getParentTable().getTableName() + "_" + index.getIndexName() + ".ser";
        serialize(index, fileName);
    }

    public static void serializePageMinMax(Page page, Tuple min, Tuple max) throws DBAppException {
        createDirectory(SERIALIZED_PAGES_MINMAX_PATH);

        String minFileName = SERIALIZED_PAGES_MINMAX_PATH + "/" + page.getPageName() + "MIN" + ".ser";
        serialize(min, minFileName);
        String maxFileName = SERIALIZED_PAGES_MINMAX_PATH + "/" + page.getPageName() + "MAX" + ".ser";
        serialize(max, maxFileName);
    }

    public static Tuple deserializePageMin(String pageName) throws DBAppException {
        String minFileName = SERIALIZED_PAGES_MINMAX_PATH + "/" + pageName + "MIN" + ".ser";
        return (Tuple)deserialize(minFileName);
    }

    public static Tuple deserializePageMax(String pageName) throws DBAppException {
        String maxFileName = SERIALIZED_PAGES_MINMAX_PATH + "/" + pageName + "MAX" + ".ser";
        return (Tuple)deserialize(maxFileName);
    }

    public static Index deserializeIndex(String indexName, Table table) throws DBAppException {
        String fileName = SERIALIZED_INDICES_PATH + "/" + table.getTableName() + "_" + indexName + ".ser";
        return (Index)deserialize(fileName);
    }

    public static void serializeTable(Table table) throws DBAppException{
        createDirectory(SERIALIZED_TABLES_PATH);
        String fileName = SERIALIZED_TABLES_PATH + "/" + table.getTableName() + ".ser";
        serialize(table, fileName);
    }

    public static Table deserializeTable(String tableName) throws DBAppException{
        String fileName = SERIALIZED_TABLES_PATH + "/" + tableName + ".ser";
        return (Table)deserialize(fileName);
    }

    //takes a pageName or tableName and deletes the file in their corresponding file path
    public static void deletePage(String pageName)throws DBAppException{
        File file = new File(SERIALIZED_PAGES_PATH + "/" + pageName + ".ser");
        if (file.exists()){
            file.delete();
        }
        file = new File(SERIALIZED_PAGES_MINMAX_PATH + "/" + pageName + "MAX.ser");
        if (file.exists()){
            file.delete();
        }
        file = new File(SERIALIZED_PAGES_MINMAX_PATH + "/" + pageName + "MIN.ser");
        if (file.exists()){
            file.delete();
        }
    }
}