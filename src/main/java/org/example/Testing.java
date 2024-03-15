package org.example;

import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;

import static org.example.Page.deserializePage;

public class Testing {
    public static void serializingTest() throws DBAppException, IOException {
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
        System.out.println(p1.pageNum);
        Page p2 = deserializePage("data/serialized_pages/CityShop1.ser");
        System.out.println("Page 2 deserialized");
        System.out.println(p2.pageNum);
    }

    private static Table createTestTable() throws IOException {
        String[] colNames = {"ID", "Name", "Number", "Specialisation", "Address"};
        String[] colTypes = {"java.lang.Integer", "java.lang.String", "java.lang.Integer", "java.lang.String", "java.lang.String"};
        Hashtable<String, String> ht = new Hashtable<>();
        for(int i = 0; i < colNames.length; i++){
            ht.put(colNames[i], colTypes[i]);
        }
        String clusteringKey = "ID";
        return new Table("CityShop", clusteringKey, ht);
    }

    private static void tupleTest() throws DBAppException {
        Tuple t = new Tuple();
        t.insert("ID", 1);
        t.insert("Name", "CityShop");
        t.insert("Number", 123456);
        t.insert("Specialisation", "Grocery");
        t.insert("Address", "Cairo");
        t.remove("Address");
        t.replace("Specialisation", "Grocery Store");
        System.out.println(t);
    }

    private static void insertIntoTableTest() throws DBAppException, IOException {
        DBApp dbApp = new DBApp();
        dbApp.init();
        Table table1 = createTestTable();

        //valid tuple
        Tuple t = new Tuple();
        t.insert("ID", 1);
        t.insert("Name", "CityShop");
        t.insert("Number", 123456);
        t.insert("Specialisation", "Grocery");
        t.insert("Address", "Cairo");
        table1.insertIntoTable(t);

        Tuple t3 = new Tuple();
        t3.insert("ID", 3);
        t3.insert("Name", "CityShop");
        t3.insert("Number", 123456);
        t3.insert("Specialisation", "Grocery");
        t3.insert("Address", "Cairo");
        table1.insertIntoTable(t3);

        Tuple t2 = new Tuple();
        t2.insert("ID", 2);
        t2.insert("Name", "CityShop");
        t2.insert("Number", 123456);
        t2.insert("Specialisation", "Grocery");
        t2.insert("Address", "Cairo");
        table1.insertIntoTable(t2);

        System.out.println(table1.pageNames.get(0));
        System.out.println(table1.pageNames.get(1));

        try {
            //wrong key
            Tuple x1 = new Tuple();
            x1.insert("ID", 2);
            x1.insert("ShopName", "CityShop");
            x1.insert("Number", 123456);
            x1.insert("Specialisation", "Grocery");
            x1.insert("Address", "Cairo");
            table1.insertIntoTable(x1);
        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }

        try {
            //wrong type
            Tuple x2 = new Tuple();
            x2.insert("ID", 2);
            x2.insert("Name", 123);
            x2.insert("Number", 123456);
            x2.insert("Specialisation", "Grocery");
            x2.insert("Address", "Cairo");
            table1.insertIntoTable(x2);
        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }

        try {
            Tuple x3 = new Tuple();
            x3.insert("ID", 1);
            x3.insert("Name", "CityShop");
            x3.insert("Number", 123456);
            x3.insert("Specialisation", "Grocery");
            x3.insert("Address", "Cairo");
            table1.insertIntoTable(x3);
        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }
    }

//    public static void sqlTermTest() throws DBAppException, IOException, CsvValidationException {
//        SQLTerm[] arrSQLTerms = new SQLTerm[5];
//
//        // valid
//        arrSQLTerms[0] = new SQLTerm("CityShop", "Name", "=", "John Noor");
//
//        // table doesn't exist
//        try {
//            arrSQLTerms[1] = new SQLTerm("Something", "Name", "=", "John Noor");
//        } catch (DBAppException e) {
//            System.out.println(e.getMessage());
//        }
//
//        // column doesn't exist
//        try {
//            arrSQLTerms[2] = new SQLTerm("CityShop", "Something", "=", "John Noor");
//        } catch (DBAppException e) {
//            System.out.println(e.getMessage());
//        }
//
//        // object datatype doesn't match column datatype
//        try {
//            arrSQLTerms[3] = new SQLTerm("CityShop", "Name", "=", Integer.valueOf(2));
//        } catch (DBAppException e) {
//            System.out.println(e.getMessage());
//        }
//
//        // illegal operator
//        try {
//            arrSQLTerms[4] = new SQLTerm("CityShop", "Name", "=>", "John Noor");
//        } catch (DBAppException e) {
//            System.out.println(e.getMessage());
//        }
//    }

    public static void main(String[] args) throws DBAppException, IOException, CsvValidationException {
        insertIntoTableTest();
    }
}
