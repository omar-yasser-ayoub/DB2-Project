package org.example;

public class Testing {
    public static void serializingTest() throws DBAppException {

        Table table1 = createTestTable();
        Page page1 = new Page(table1, 200);
        Page page2 = new Page(table1, 200);

        page1.serializePage();
        page2.serializePage();

    }

    private static Table createTestTable() {
        String[] columnNames = {"id", "name", "gpa"};
        String[] columnTypes = {"java.lang.Integer", "java.lang.String", "java.lang.Double"};
        boolean[] clusteringKey = {true, false, false};
        String[] indexName = {"idIndex", "nameIndex", "gpaIndex"};
        String[] indexType = {"B+tree", "B+tree", "B+tree"};
        return new Table("table1", columnNames, columnTypes, clusteringKey, indexName, indexType);
    }

    public static void main(String[] args) throws DBAppException {
        serializingTest();
    }
}
