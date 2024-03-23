package org.example;

import java.io.*;

public class FileManager implements Serializable {

    public static final String SERIALIZED_PAGES_PATH = "data/serialized_pages";
    public static final String SERIALIZED_INDICES_PATH = "data/serialized_indices";

    private FileManager() {
        throw new IllegalStateException("Utility class");
    }

    private static void createSerializedDirectory(String dirPath) throws DBAppException {
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
            throw new DBAppException(e.getMessage());
        }
    }

    public static void serializePage(Page page) throws DBAppException {
        createSerializedDirectory(SERIALIZED_PAGES_PATH);
        String fileName = SERIALIZED_PAGES_PATH + page.getParentTable().getTableName() + page.getPageNum() + ".ser";
        serialize(page, fileName);
    }

    public static Page deserializePage(String pageName) throws DBAppException {
        String fileName = SERIALIZED_PAGES_PATH + pageName + ".ser";
        return (Page)deserialize(fileName);
    }

    public static void serializeIndex(Index index) throws DBAppException {
        createSerializedDirectory(SERIALIZED_INDICES_PATH);
        String fileName = SERIALIZED_INDICES_PATH + index.getParentTable().getTableName() + index.getColumnName() + ".ser";
        serialize(index, fileName);
    }

    public static Index deserializeIndex(String indexName) throws DBAppException {
        String fileName = SERIALIZED_INDICES_PATH + indexName + ".ser";
        return (Index)deserialize(fileName);
    }
}