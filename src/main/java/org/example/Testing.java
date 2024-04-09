package org.example;

import com.opencsv.exceptions.CsvValidationException;
import org.example.data_structures.Page;
import org.example.data_structures.SQLTerm;
import org.example.data_structures.Table;
import org.example.data_structures.Tuple;
import org.example.data_structures.index.Index;
import org.example.data_structures.index.IntegerIndex;
import org.example.exceptions.DBAppException;
import org.example.managers.FileManager;
import org.example.managers.SelectionManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

import static org.example.managers.FileManager.deserializePage;
import static org.example.managers.FileManager.deserializeTable;

public class Testing {
    public static void serializingTest() throws DBAppException, IOException, CsvValidationException {
        DBApp dbApp = new DBApp();
        dbApp.init();

        Table table1 = createTestTable();
        Page page1 = new Page(table1, 0);
        Page page2 = new Page(table1, 1);

        page1.save();
        System.out.println("Page 1 serialized");
        page2.save();
        System.out.println("Page 2 serialized");
        table1.save();
        System.out.println("Table1 serialized");
    }

    private static void deserializingTest() throws DBAppException {
        Page p1 = FileManager.deserializePage("CityShop0");
        System.out.println("Page 1 deserialized");
        System.out.println(p1.getPageNum());
        Page p2 = FileManager.deserializePage("CityShop1");
        System.out.println("Page 2 deserialized");
        System.out.println(p2.getPageNum());
        Table t = FileManager.deserializeTable("CityShop");
        System.out.println("Table1 deserialized");
        System.out.println(t.getTableName());
        System.out.println(t.getPageNames());

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
        int numOfTestTuples = 50;

        List<Integer> numbers = new ArrayList<>();
        for (int i = 1; i <= numOfTestTuples; i++) {
            numbers.add(i);
        }
        Collections.shuffle(numbers);

//        numbers.clear();
//        numbers.add(45);
//        numbers.add(14);
//        numbers.add(30);
//        numbers.add(36);

        for (int num : numbers) {
            Tuple t = new Tuple();
            t.insert("ID", num);
            t.insert("Name", "City Shop");
            t.insert("Number", num);
            t.insert("Specialisation", "");
            t.insert("Address", "");
            System.out.println("Inserting tuple " + num);
            //System.out.println("Index " + numbers.indexOf(num));
            table1.insert(t);
        }
        for (String pageName : table1.getPageNames()) {
            System.out.println(deserializePage(pageName));
        }

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
        return table1;
    }
    public static void sqlTermTest() throws DBAppException, IOException, CsvValidationException {
        SQLTerm[] arrSQLTerms = new SQLTerm[5];

        // valid
        arrSQLTerms[0] = new SQLTerm("CityShop", "Name", "=", "John Noor");

        // table doesn't exist
        try {
            arrSQLTerms[1] = new SQLTerm("Something", "Name", "=", "John Noor");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        // column doesn't exist
        try {
            arrSQLTerms[2] = new SQLTerm("CityShop", "Something", "=", "John Noor");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        // object datatype doesn't match column datatype
        try {
            arrSQLTerms[3] = new SQLTerm("CityShop", "Name", "=", Integer.valueOf(2));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        // illegal operator
        try {
            arrSQLTerms[4] = new SQLTerm("CityShop", "Name", "=>", "John Noor");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    private static void deleteFromTableTest() throws DBAppException, CsvValidationException, IOException {
        DBApp dbApp = new DBApp();
        dbApp.init();
        Table table1 = insertIntoTableTest();

        int numOfTestTuples = 40;

        List<Integer> numbers = new ArrayList<>();
        for (int i = 1; i <= numOfTestTuples; i++) {
            numbers.add(i);
        }
        Collections.shuffle(numbers);

//        numbers.clear();
//        numbers.add(9);
//        numbers.add(5);
//        numbers.add(2);
//        numbers.add(1);
//        numbers.add(7);
//        numbers.add(3);
//        numbers.add(6);

        for (int num : numbers) {
            Tuple t = new Tuple();
            t.insert("ID", num);
            t.insert("Name", "City Shop");
            t.insert("Number", num);
            t.insert("Specialisation", "");
            t.insert("Address", "");
            System.out.println("Deleting tuple " + num);
            table1.delete(t);
//            System.out.println("Table after deletion:");
//            for (String pageName : table1.getPageNames()) {
//                System.out.println(deserializePage(pageName));
//            }
        }
        for (String pageName : table1.getPageNames()) {
            System.out.println(deserializePage(pageName));
        }
    }
    private static void IndexTest() throws DBAppException, CsvValidationException, IOException {
        DBApp dbApp1 = new DBApp();
        dbApp1.init();
        Table table = createTestTable();
        Index index = new IntegerIndex(table, "Number", "NumIndex");
        for (int i = 0; i < 50; i++) {
            index.insert(i, String.valueOf(i));
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

            for (String pageName : tableBin.getPageNames()) {
                System.out.println(deserializePage(pageName));
            }
            tableBin.delete(x2);
            for (String pageName : tableBin.getPageNames()) {
                System.out.println(deserializePage(pageName));
            }
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

    private static void selectEvalTest() throws DBAppException {
        Tuple t1 = new Tuple();
        t1.insert("ID", 1); t1.insert("Name", "Farah"); t1.insert("Number", 70);
        Tuple t2 = new Tuple();
        t2.insert("ID", 2); t2.insert("Name", "Reeka"); t2.insert("Number", 99);
        Tuple t3 = new Tuple();
        t3.insert("ID", 3); t3.insert("Name", "Ziad"); t3.insert("Number", 46);
        Tuple t4 = new Tuple();
        t4.insert("ID", 4); t4.insert("Name", "Yara"); t4.insert("Number", 31);
        Tuple t5 = new Tuple();
        t5.insert("ID", 5); t5.insert("Name", "Jana"); t5.insert("Number", 14);
        Tuple t6 = new Tuple();
        t6.insert("ID", 6); t6.insert("Name", "Amgad"); t6.insert("Number", 90);
        Tuple t7 = new Tuple();
        t7.insert("ID", 7); t7.insert("Name", "Fofo"); t7.insert("Number", 85);
        Tuple t8 = new Tuple();
        t8.insert("ID", 8); t8.insert("Name", "Salma"); t8.insert("Number", 83);
        Tuple t9 = new Tuple();
        t9.insert("ID", 9); t9.insert("Name", "Wael"); t9.insert("Number", 61);
        Tuple t10 = new Tuple();
        t10.insert("ID", 10); t10.insert("Name", "Fatima"); t10.insert("Number", 94);

        // Sample data
        Vector<Tuple> tuples1 = new Vector<>();
        tuples1.add(t1);
        tuples1.add(t3);
        Vector<Tuple> tuples2 = new Vector<>();
        tuples2.add(t1);
        tuples2.add(t3);
        tuples2.add(t4);
        tuples2.add(t9);
        Vector<Tuple> tuples3 = new Vector<>();
        tuples3.add(t1);
        tuples3.add(t3);
        tuples3.add(t5);
        tuples3.add(t9);
        Vector<Tuple> tuples4 = new Vector<>();
        tuples4.add(t1);
        tuples4.add(t2);
        tuples4.add(t4);
        tuples4.add(t6);
        tuples4.add(t9);
        Vector<Tuple> tuples5 = new Vector<>();
        tuples5.add(t1);
        tuples5.add(t4);

        // List of all tuples
        Vector<Vector<Tuple>> allTuples = new Vector<>();
        allTuples.add(tuples1);
        allTuples.add(tuples2);
        allTuples.add(tuples3);
        allTuples.add(tuples4);
        allTuples.add(tuples5);

        // Operators
        String[] strarrOperators = {"OR", "AND", "XOR", "AND"};

        // Evaluate query
        Vector<Tuple> result = SelectionManager.evalQuery(allTuples, strarrOperators);

        // Output the result
        for(Tuple tuple : result) {
            System.out.println(tuple);
        }
    }

    public static void deserializeEqCheck() throws DBAppException, CsvValidationException, IOException {
//        Table t = createTestTable();
//        Tuple t1 = new Tuple();
//        t1.insert("ID", 1); t1.insert("Name", "Farah"); t1.insert("Number", 21);
//        t1.insert("Specialisation", "MET"); t1.insert("Address", "123s");
//        Tuple t2 = new Tuple();
//        t2.insert("ID", 2); t2.insert("Name", "Ahmad"); t2.insert("Number", 30);
//        t2.insert("Specialisation", "ENG"); t2.insert("Address", "456b");
//        Tuple t3 = new Tuple();
//        t3.insert("ID", 3); t3.insert("Name", "Mohamed"); t3.insert("Number", 38);
//        t3.insert("Specialisation", "BI"); t3.insert("Address", "789v");
//        t.insert(t1); t.insert(t2); t.insert(t3);
//        t.save();

        Table newT = deserializeTable("CityShop");
        Page p1 = deserializePage(newT.getPageNames().get(0));
        Page p2 = deserializePage(newT.getPageNames().get(0));

        Vector<Tuple> p1Tuples = p1.getTuples();

        Hashtable<String, Object> ht1 = p1.getTuples().get(0).getValues();
        Hashtable<String, Object> ht2 = p2.getTuples().get(0).getValues();

        Vector<Hashtable<String, Object>> htVector = new Vector<>();

        for(Tuple t : p1Tuples) {
            htVector.add(t.getValues());
        }

        System.out.println(p1.getTuples().contains(p2.getTuples().get(0)));
        // ^ prints false
        System.out.println(ht1.equals(ht2));
        // ^ prints true
        System.out.println(htVector.contains(p2.getTuples().get(0).getValues()));
        // ^ also prints true
    }

    public static void main(String[] args) throws Exception {
        deleteFromTableTest();
    }
}
