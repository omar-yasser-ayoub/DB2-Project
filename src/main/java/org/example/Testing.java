package org.example;

import java.util.Hashtable;

import static org.example.Page.deserializePage;

public class Testing {
    public static void serializingTest() throws DBAppException {

        DBApp dbApp = new DBApp();
        dbApp.init();

        Table table1 = createTestTable();
        Page page1 = new Page(table1, 200);
        Page page2 = new Page(table1, 200);

        page1.serializePage();
        System.out.println("Page 1 serialized");
        page2.serializePage();
        System.out.println("Page 2 serialized");

    }
    private static void deserializingTest() throws DBAppException {
        Page p1 = deserializePage("data/serialized_pages/CityShop0.ser");
        System.out.println("Page 1 deserialized");
        System.out.println(p1.numOfRows);
        Page p2 = deserializePage("data/serialized_pages/CityShop1.ser");
        System.out.println("Page 2 deserialized");
        System.out.println(p2.numOfRows);    }

    private static Table createTestTable() {
        String[] colNames = {"ID", "Name", "Number", "Specialisation", "Address"};
        String[] colTypes = {"java.lang.Integer", "java.lang.String", "java.lang.Integer", "java.lang.String", "java.lang.String"};
        Hashtable<String, String> ht = new Hashtable<>();
        for(int i = 0; i < colNames.length; i++){
            ht.put(colNames[i], colTypes[i]);
        }
        String clusteringKey = "ID";
        return new Table("CityShop", clusteringKey, ht);
    }

    public static void main(String[] args) throws DBAppException {
        serializingTest();
        deserializingTest();
    }
}
