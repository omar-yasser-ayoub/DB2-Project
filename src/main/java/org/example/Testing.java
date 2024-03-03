package org.example;

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

    private static Table createTestTable() {
        String[] colNames = {"ID", "Name", "Number", "Specialisation", "Address"};
        String[] colTypes = {"java.lang.Integer", "java.lang.String", "java.lang.Integer", "java.lang.String", "java.lang.String"};
        boolean[] clusteringKey = {true, false, false, false, false};
        String[] indexName = {"IDIndex", null, "NumberIndex", "SpecIndex", "AddrIndex"};
        String[] indexType = {"B+tree", null, "B+tree", "B+tree", "B+tree"};
        return new Table("CityShop", colNames, colTypes, clusteringKey, indexName, indexType);
    }

    public static void main(String[] args) throws DBAppException {
        serializingTest();
    }
}
