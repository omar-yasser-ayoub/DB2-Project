package org.example;

import com.opencsv.exceptions.CsvValidationException;
import org.example.data_structures.Page;
import org.example.data_structures.Table;
import org.example.data_structures.Tuple;
import org.example.exceptions.DBAppException;
import org.example.managers.FileManager;

import java.io.IOException;
import java.util.Hashtable;

import static org.example.managers.FileManager.deserializePage;

public class Testing {
    public static void serializingTest() throws DBAppException, IOException, CsvValidationException {
        DBApp dbApp = new DBApp();
        dbApp.init();

        Table table1 = createTestTable();
        Page page1 = new Page(table1, 200);
        Page page2 = new Page(table1, 200);

        page1.save();
        System.out.println("Page 1 serialized");
        page2.save();
        System.out.println("Page 2 serialized");
    }

    private static void deserializingTest() throws DBAppException {
        Page p1 = FileManager.deserializePage("CityShop0");
        System.out.println("Page 1 deserialized");
        System.out.println(p1.getPageNum());
        Page p2 = FileManager.deserializePage("CityShop1");
        System.out.println("Page 2 deserialized");
        System.out.println(p2.getPageNum());
    }

    private static Table createTestTable() throws IOException, CsvValidationException, DBAppException {
        DBApp dbApp = new DBApp();
        dbApp.init();
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

    private static Table insertIntoTableTest() throws DBAppException, IOException, CsvValidationException {
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
        table1.insert(t);

        Tuple t3 = new Tuple();
        t3.insert("ID", 3);
        t3.insert("Name", "CityShop2");
        t3.insert("Number", 123456);
        t3.insert("Specialisation", "Grocery");
        t3.insert("Address", "Cairo");
        table1.insert(t3);

        Tuple t2 = new Tuple();
        t2.insert("ID", 2);
        t2.insert("Name", "CityShop3");
        t2.insert("Number", 123456);
        t2.insert("Specialisation", "Grocery");
        t2.insert("Address", "Cairo");
        table1.insert(t2);

        System.out.println(deserializePage(table1.getPageNames().get(0)));
        System.out.println(deserializePage(table1.getPageNames().get(1)));

        try {
            //wrong key
            Tuple x1 = new Tuple();
            x1.insert("ID", 2);
            x1.insert("ShopName", "CityShop");
            x1.insert("Number", 123456);
            x1.insert("Specialisation", "Grocery");
            x1.insert("Address", "Cairo");
            table1.insert(x1);
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
            table1.insert(x2);
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
            table1.insert(x3);
        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }
        return table1;
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
    private static void deleteFromTableTest() throws DBAppException, CsvValidationException, IOException {
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
        table1.insert(t);

        Tuple t3 = new Tuple();
        t3.insert("ID", 3);
        t3.insert("Name", "CityShop2");
        t3.insert("Number", 123456);
        t3.insert("Specialisation", "Grocery");
        t3.insert("Address", "Cairo");
        table1.insert(t3);

        Tuple t2 = new Tuple();
        t2.insert("ID", 2);
        t2.insert("Name", "CityShop3");
        t2.insert("Number", 123456);
        t2.insert("Specialisation", "Grocery");
        t2.insert("Address", "Cairo");
        table1.insert(t2);

        System.out.println(deserializePage(table1.getPageNames().get(0)));
        System.out.println(deserializePage(table1.getPageNames().get(1)));

        try{
        table1.delete(t);
        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }

        System.out.println(deserializePage(table1.getPageNames().get(0)));
        System.out.println(deserializePage(table1.getPageNames().get(1)));

    }

    private static void BinarySearchTest() throws DBAppException, IOException, CsvValidationException {
        DBApp dbApp1 = new DBApp();
        dbApp1.init();
        Table tableBin = createTestTable();

        //valid tuple
        Tuple t = new Tuple();
        t.insert("ID", 1);
        t.insert("Name", "CityShop");
        t.insert("Number", 123456);
        t.insert("Specialisation", "Grocery");
        t.insert("Address", "Cairo");
        tableBin.insert(t);
        System.out.println("Done inserting");

        Tuple t2 = new Tuple();
        t2.insert("ID", 2);
        t2.insert("Name", "Shop");
        t2.insert("Number", 123);
        t2.insert("Specialisation", "Grocery");
        t2.insert("Address", "Cairo");
        tableBin.insert(t2);
        System.out.println("Done inserting");


        //System.out.println(deserializePage(tableBin.pageNames.get(0)));

        try {



            Tuple x1 = new Tuple();
            x1.insert("ID", 4);
            x1.insert("Name", "Shop");
            x1.insert("Number", 11);
            x1.insert("Specialisation", "Grocery");
            x1.insert("Address", "Cairo");
            tableBin.insert(x1);
            System.out.println("Done inserting");


            Tuple x2 = new Tuple();
            x2.insert("ID", 3);
            x2.insert("Name", "CityShop");
            x2.insert("Number", 12);
            x2.insert("Specialisation", "Grocery");
            x2.insert("Address", "Cairo");
            tableBin.insert(x2);
            System.out.println("Done inserting");


            //boolean b = tableBin.tupleHasNoDuplicateClusteringKey("Number",(Object)x1);

            /*Tuple x2 = new Tuple();
            x2.insert("ID", 3);
            x2.insert("Name", "CityShop");
            x2.insert("Number", 3456);
            x2.insert("Specialisation", "Grocery");
            x2.insert("Address", "Cairo");
            tableBin.insertIntoTable(x2);
            //Tuple valt = (Tuple)x2;
            //boolean b2 = tableBin.tupleHasNoDuplicateClusteringKey("ID",(Object)3);
            //Tuple valt = (Tuple)x2;
            //int b2 = compareObjects(valt.getValues().get("ID"),)
            //System.out.println(b);
           // System.out.println(b2);*/
        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }


    }
    private static void BinarySearchTest() throws DBAppException, IOException, CsvValidationException {
        DBApp dbApp1 = new DBApp();
        dbApp1.init();
        Table tableBin = createTestTable();

        //valid tuple
        Tuple t = new Tuple();
        t.insert("ID", 1);
        t.insert("Name", "CityShop");
        t.insert("Number", 123456);
        t.insert("Specialisation", "Grocery");
        t.insert("Address", "Cairo");
        tableBin.insertIntoTable(t);
        System.out.println("Done inserting");

        Tuple t2 = new Tuple();
        t2.insert("ID", 2);
        t2.insert("Name", "Shop");
        t2.insert("Number", 123);
        t2.insert("Specialisation", "Grocery");
        t2.insert("Address", "Cairo");
        tableBin.insertIntoTable(t2);
        System.out.println("Done inserting");


        //System.out.println(deserializePage(tableBin.pageNames.get(0)));

        try {



            Tuple x1 = new Tuple();
            x1.insert("ID", 4);
            x1.insert("Name", "Shop");
            x1.insert("Number", 11);
            x1.insert("Specialisation", "Grocery");
            x1.insert("Address", "Cairo");
            tableBin.insertIntoTable(x1);
            System.out.println("Done inserting");


            Tuple x2 = new Tuple();
            x2.insert("ID", 3);
            x2.insert("Name", "CityShop");
            x2.insert("Number", 12);
            x2.insert("Specialisation", "Grocery");
            x2.insert("Address", "Cairo");
            tableBin.insertIntoTable(x2);
            System.out.println("Done inserting");


            //boolean b = tableBin.tupleHasNoDuplicateClusteringKey("Number",(Object)x1);

            /*Tuple x2 = new Tuple();
            x2.insert("ID", 3);
            x2.insert("Name", "CityShop");
            x2.insert("Number", 3456);
            x2.insert("Specialisation", "Grocery");
            x2.insert("Address", "Cairo");
            tableBin.insertIntoTable(x2);
            //Tuple valt = (Tuple)x2;
            //boolean b2 = tableBin.tupleHasNoDuplicateClusteringKey("ID",(Object)3);
            //Tuple valt = (Tuple)x2;
            //int b2 = compareObjects(valt.getValues().get("ID"),)
            //System.out.println(b);
           // System.out.println(b2);*/
        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }


    }
    public static void main(String[] args) throws Exception {
        BinarySearchTest();
    }
}
