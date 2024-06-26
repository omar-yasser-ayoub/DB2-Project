package org.example;

import com.opencsv.exceptions.CsvValidationException;
import org.example.data_structures.Page;
import org.example.data_structures.SQLTerm;
import org.example.data_structures.Table;
import org.example.data_structures.Tuple;
import org.example.exceptions.DBAppException;
import org.example.managers.FileManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

public class UnitTest {
    private static DBApp engine;
    private static String newTableName;
    private static final String id = "id";
    private static final String name = "name";
    private static final String gpa = "gpa";
    private static final String TEST_NAME = "Abdo";
    private static final double TEST_GPA = 1.8;
    private static final String STRING_DATA_TYPE_NAME = "java.lang.String";
    private static final String INTEGER_DATA_TYPE_NAME = "java.lang.Integer";
    private static final String DOUBLE_DATA_TYPE_NAME = "java.lang.Double";

    private static void generateNewTableName() {
        int randomNumber1 = (int) (Math.random() * 100000) + 1;
        int randomNumber2 = (int) (Math.random() * 100000) + 1;
        StringBuilder s = new StringBuilder();
        newTableName = s.append("aa").append(randomNumber1).append(randomNumber2).toString();
        while (engine.getMyTables().contains(newTableName)) {
            randomNumber1 = (int) (Math.random() * 100000) + 1;
            randomNumber2 = (int) (Math.random() * 100000) + 1;
            s.setLength(0);
            newTableName = s.append(randomNumber1).append(randomNumber2).toString();
        }
    }
    private static Hashtable<String, String> createHashtable(String value1, String value2, String value3) {
        Hashtable<String, String> hashtable = new Hashtable<String, String>();
        hashtable.put(id, value1);
        hashtable.put(name, value2);
        hashtable.put(gpa, value3);
        return hashtable;
    }

    private static Hashtable<String, Object> createRow(int idInput, String nameInput, double gpaInput) {
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(id, idInput);
        htblColNameValue.put(name, nameInput);
        htblColNameValue.put(gpa, gpaInput);
        return htblColNameValue;
    }
    private static void createTable() throws DBAppException, CsvValidationException, IOException {
        Hashtable<String, String> htblColNameType = createHashtable(INTEGER_DATA_TYPE_NAME,
                STRING_DATA_TYPE_NAME, DOUBLE_DATA_TYPE_NAME);

        engine.createTable(newTableName, id, htblColNameType);
    }

    @BeforeEach
    void setEnvironment() throws DBAppException, CsvValidationException, IOException {
        engine = new DBApp();
        engine.init();
        generateNewTableName();
        createTable();
    }

    @Test
    void testCreateTable_AlreadyExistingName_ShouldFailCreation() throws DBAppException {
        // Given
        Hashtable<String, String> htblColNameType = new Hashtable<>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("courseName", "java.lang.String");
        // When
        Exception exception = assertThrows(DBAppException.class, () ->
                engine.createTable(newTableName, "id", htblColNameType)
        );
        // Then
        assertEquals("Table already exists", exception.getMessage());
    }
    @Test
    void testCreateTable_InvalidPrimaryKeyColumn_ShouldFailCreation() throws DBAppException {
        // Given
        Hashtable<String, String> htblColNameType = new Hashtable<>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("courseName", "java.lang.String");
        // When
        Exception exception = assertThrows(DBAppException.class, () ->
                engine.createTable("newTable", "price", htblColNameType)
        );
        // Then
        assertEquals("Clustering key not found in table", exception.getMessage());
    }

    @Test
    void testCreateTable_InvalidDataType_ShouldFailCreation() throws DBAppException {
        // Given
        Hashtable<String, String> htblColNameType = new Hashtable<>();
        htblColNameType.put("id", "java.lang.Byte");
        htblColNameType.put("courseName", "java.lang.String");
        // When
        Exception exception = assertThrows(DBAppException.class, () ->
                engine.createTable("newTable", "id", htblColNameType)
        );
        // Then
        assertEquals("Invalid column datatype", exception.getMessage());
    }


    @Test
    void testInsertIntoTable_OneTuple_ShouldInsertSuccessfully()
            throws DBAppException, ClassNotFoundException, IOException {
        // Given
        Hashtable<String, Object> htblColNameValue = createRow(1, TEST_NAME, TEST_GPA);

        // When
        engine.insertIntoTable(newTableName, htblColNameValue);

        // Then
        Table table = FileManager.deserializeTable(newTableName);
        assertEquals(1, Objects.requireNonNull(table).getPageNames().size());
        Page page = table.getPageAtPosition(0);
        assertEquals(1, page.getSize());
    }


    @Test
    void testInsertIntoTable_MissingColumn_ShouldInsertSuccessfully()
            throws DBAppException, ClassNotFoundException, IOException {
        // Given
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(gpa, TEST_GPA);
        htblColNameValue.put(id, 5);

        // When
        engine.insertIntoTable(newTableName, htblColNameValue);

        // Then
        Table table = FileManager.deserializeTable(newTableName);
        assertEquals(1,table.getPageNames().size());
        Page page = table.getPageAtPosition(0);
        assertEquals(1, page.getSize());
    }


    @Test
    void testInsertIntoTable_ManyTuples_ShouldInsertSuccessfully() throws DBAppException {

        for (int i = 1; i < 300; i++) {
            // Given
            Hashtable<String, Object> htblColNameValue = createRow(i, TEST_NAME, TEST_GPA);

            // When
            engine.insertIntoTable(newTableName, htblColNameValue);
        }
        // Then
        Table table = FileManager.deserializeTable(newTableName);
        assertEquals(2, table.getPageNames().size());
        Page page = table.getPageAtPosition(1);
        assertEquals(99, page.getSize());
        page = table.getPageAtPosition(0);
        assertTrue(page.isFull());
    }

    @Test
    void testInsertIntoTable_InsertingLastRecordIntoFullPage_ShouldInsertSuccessfully() throws DBAppException {
        // Given
        for (int i = 2; i < 402; i += 2) {
            Hashtable<String, Object> htblColNameValue = createRow(i, TEST_NAME, TEST_GPA);
            engine.insertIntoTable(newTableName, htblColNameValue);
        }

        // When
        Hashtable<String, Object> htblColNameValue = createRow(399, TEST_NAME, TEST_GPA);
        engine.insertIntoTable(newTableName, htblColNameValue);

        // Then
        Table table = FileManager.deserializeTable(newTableName);
        assertEquals(2, table.getPageNames().size());
        Page page = table.getPageAtPosition(0);
        assertTrue(page.isFull());
        assertEquals(399,page.getTuples().get(199).getPrimaryKeyValue());
    }

    @Test
    void testInsertIntoTable_InsertingRecordShiftingTwoPages_ShouldInsertSuccessfully() throws DBAppException {
        // Given
        for (int i = 2; i <= 802; i += 2) {
            Hashtable<String, Object> htblColNameValue = createRow(i, TEST_NAME, TEST_GPA);
            engine.insertIntoTable(newTableName, htblColNameValue);
        }

        // When
        Hashtable<String, Object> htblColNameValue = createRow(399, TEST_NAME, TEST_GPA);
        engine.insertIntoTable(newTableName, htblColNameValue);

        // Then
        Table table = FileManager.deserializeTable(newTableName);
        assertEquals(3, table.getPageNames().size());
        Page page = table.getPageAtPosition(0);
        assertEquals(399,page.getTuples().get(199).getPrimaryKeyValue());
        page = table.getPageAtPosition(1);
        assertEquals(400,page.getTuples().get(0).getPrimaryKeyValue());
        page = table.getPageAtPosition(2);
        assertEquals(800,page.getTuples().get(0).getPrimaryKeyValue());
    }

    @Test
    void testInsertIntoTable_RepeatedPrimaryKey_ShouldFailInsert()
            throws DBAppException {
        // Given
        insertRow(1);
        Hashtable<String, Object> htblColNameValue = createRow(1, "moham", TEST_GPA);

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.insertIntoTable(newTableName, htblColNameValue);
        });

        // Then
        String expectedMessage = "Primary key already exists in table";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage,outputMessage);
    }

    @Test
    void testInsertIntoTable_InvalidDataType_ShouldFailInsertion() throws DBAppException {
        // Given
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(name, "Foo");
        htblColNameValue.put(gpa, "boo");
        htblColNameValue.put(id, 55);

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.insertIntoTable(newTableName, htblColNameValue);
        });

        // Then
        String expectedMessage = "Value is not of the correct type";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage,outputMessage);
    }

    @Test
    void testInsertIntoTable_MissingPrimaryKey_ShouldFailInsert() throws DBAppException {
        // Given
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(gpa, TEST_GPA);
        htblColNameValue.put(name, TEST_NAME);

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.insertIntoTable(newTableName, htblColNameValue);
        });

        // Then
        String expectedMessage = "Tuple must contain primary key";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage,outputMessage);
    }

    @Test
    void testInsertIntoTable_InvalidTableName_ShouldFailInsertion() {
        // Given
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(name, "Foo");
        htblColNameValue.put(gpa, TEST_GPA);
        htblColNameValue.put(id, 55);

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.insertIntoTable("someRandomTableName", htblColNameValue);
        });

        // Then
        String expectedMessage = "Table does not exist";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage,outputMessage);
    }

    @Test
    void testInsertIntoTable_ExtraColumn_ShouldFailInsertion() {
        // Given
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(name, "Foo");
        htblColNameValue.put("salary", 10000);
        htblColNameValue.put(gpa, TEST_GPA);
        htblColNameValue.put(id, 3);

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.insertIntoTable(newTableName, htblColNameValue);
        });

        // Then
        String expectedMessage = "Key is not found in table";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage,outputMessage);
    }

    @Test
    void testUpdateTable_ValidInput_ShouldUpdateSuccessfully()
            throws DBAppException, ClassNotFoundException, IOException {
        // Given
        insertRow(1);
        String updatedName = "moham";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(name, updatedName);

        // When
        engine.updateTable(newTableName, "1", htblColNameValue);

        // Then
        String pageName = FileManager.deserializeTable(newTableName).getPageNames().get(0);
        Page page = FileManager.deserializePage(pageName);
        Tuple updated = page.getTuples().get(0);
        assertEquals(updatedName,updated.getValues().get(name));
    }

    @Test
    void testUpdateTable_PrimaryKeyUpdate_ShouldFailUpdate()
            throws DBAppException, ClassNotFoundException, IOException {
        // Given
        insertRow(1);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(id, 2);

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.updateTable(newTableName, "1", htblColNameValue);
        });

        // Then
        String expectedMessage = "Cannot change primary key";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage, outputMessage);
    }

    @Test
    void testUpdateTable_ExtraInput_ShouldFailUpdate() throws DBAppException {
        // Given
        insertRow(1);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(name, "Foo");
        htblColNameValue.put(gpa, 1.8);
        htblColNameValue.put("University", "GUC");

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.updateTable(newTableName, "0", htblColNameValue);
        });

        // Then
        String expectedMessage = "Hashtable contains extra column(s)";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage, outputMessage);
    }

    @Test
    void testUpdateTable_InvalidDataType_ShouldFailUpdate() throws DBAppException {
        // Given
        insertRow(1);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(gpa, "Foo");

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.updateTable(newTableName, "1", htblColNameValue);
        });

        // Then
        String expectedMessage = "Column value doesn't match datatype";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage, outputMessage);
    }

    @Test
    void testUpdateTable_InvalidTableName_ShouldFailUpdate() throws DBAppException {
        // Given
        insertRow(1);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(gpa, 1.8);

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.updateTable("randomName", "1", htblColNameValue);
        });

        // Then
        String expectedMessage = "Table does not exist";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage, outputMessage);
    }

    @Test
    void testDeleteFromTable_OneTuple_ShouldDeleteSuccessfully()
            throws DBAppException, ClassNotFoundException, IOException, InterruptedException {
        // Given
        insertRow(1);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(id, 1);

        // When
        engine.deleteFromTable(newTableName, htblColNameValue);

        // Then
        Table table = FileManager.deserializeTable(newTableName);
        assertTrue(table.isEmpty());
    }

    @Test
    void testDeleteFromTable_ManyTuplesDeleteOne_ShouldDeleteSuccessfully()
            throws DBAppException, ClassNotFoundException, IOException {
        // Given
        for (int i = 0; i < 100; i++)
            insertRow(i);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(name, TEST_NAME);
        htblColNameValue.put(id, 0);

        // When
        engine.deleteFromTable(newTableName, htblColNameValue);

        // Then
        Table table = FileManager.deserializeTable(newTableName);
        assertEquals(99, table.getSize());
    }

    @Test
    void testDeleteFromTable_ManyTuplesDeleteAll_ShouldDeleteSuccessfully()
            throws DBAppException, ClassNotFoundException, IOException {
        // Given
        for (int i = 0; i < 100; i++)
            insertRow(i);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(name, TEST_NAME);

        // When
        engine.deleteFromTable(newTableName, htblColNameValue);

        // Then
        Table table = FileManager.deserializeTable(newTableName);
        assertTrue(table.isEmpty());
    }

    @Test
    void testDeleteFromTable_InvalidColumnName_ShouldFailDelete()
            throws DBAppException, ClassNotFoundException, IOException {
        // Given
        insertRow(1);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("middle_name", "Mohamed");

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.deleteFromTable(newTableName, htblColNameValue);
        });

        // Then
        String expectedMessage = "The tuple contains some columns that aren't in the table";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage, outputMessage);
    }

    @Test
    void testDeleteFromTable_InvalidDataType_ShouldFailDelete() throws DBAppException {
        // Given
        insertRow(1);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(id, 1);
        htblColNameValue.put("gpa", "Foo");

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.deleteFromTable(newTableName, htblColNameValue);
        });

        // Then
        String expectedMessage = "Object datatype doesn't match column datatype";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage, outputMessage);
    }

    @Test
    void testDeleteFromTable_InvalidTable_ShouldFailDelete()
            throws DBAppException, ClassNotFoundException, IOException, InterruptedException {
        // Given
        insertRow(1);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(id, 1);

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.deleteFromTable("randomTableName", htblColNameValue);
        });

        // Then
        String expectedMessage = "Table does not exist";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage, outputMessage);
    }

	@Test
	void testCreateIndex_ValidInput_ShouldCreateSuccessfully() throws DBAppException {
		// Given
		insertRow(1);

		// When
		engine.createIndex(newTableName, gpa, gpa+"Index");

		// Then
		Table table = FileManager.deserializeTable(newTableName);
		assertEquals(1,FileManager.deserializeIndex(table.getIndices().get(0),table ).getRootKeyCount());
		assertEquals(1, table.getIndices().size());
	}

    @Test
    void testCreateIndex_RepeatedIndex_ShouldFailCreation() throws DBAppException {
        // Given
        engine.createIndex(newTableName, gpa, gpa+"Index");

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.createIndex(newTableName, name, gpa+"Index");
        });

        // Then
        String expectedMessage = "The index was already created on one of the columns";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage, outputMessage);

    }

    @Test
    void testCreateIndex_InvalidTableName_ShouldFailCreation() throws DBAppException {

        // Given

        // When
        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.createIndex("Foo", gpa, gpa);
        });

        // Then
        String expectedMessage = "Table does not exist";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage, outputMessage);
    }

	@Test
	void testInsertionIntoIndex_ValidInput_ShouldInsertIntoIndex() throws DBAppException {
		// Given
		engine.createIndex(newTableName, gpa, gpa+"Index.ser");
		Table table = FileManager.deserializeTable(newTableName);
		int oldSize = FileManager.deserializeIndex(table.getIndices().get(0),table ).getRootKeyCount();

		// When
		insertRow(1);

		// Then
		table = FileManager.deserializeTable(newTableName);
		int newSize = FileManager.deserializeIndex(table.getIndices().get(0),table ).getRootKeyCount();
		assertEquals(oldSize+1, newSize);
	}

//    Comment if your btree doesn't contain these functions/add them to yours
	@Test
	void testUpdateTable_ValidInput_ShouldUpdateIndex() throws DBAppException {
		// Given
		engine.createIndex(newTableName, gpa, gpa+"Index.ser");
		insertRow(3);
		Table table = FileManager.deserializeTable(newTableName);
		boolean oldValue = (FileManager.deserializeIndex(table.getIndices().get(0), table)).checkKeyExists(TEST_GPA);

		// When
		Hashtable<String, Object> updateTable = new Hashtable<>();
		updateTable.put("gpa", 0.7);
		engine.updateTable(newTableName, "3", updateTable);

		// Then
		table = FileManager.deserializeTable(newTableName);
		boolean oldValueCheck = (FileManager.deserializeIndex(table.getIndices().get(0), table)).checkKeyExists(TEST_GPA);
		boolean newValueCheck = (FileManager.deserializeIndex(table.getIndices().get(0),table )).checkKeyExists(0.7);
		assertTrue(oldValue);
		assertFalse(oldValueCheck);
		assertTrue(newValueCheck);
	}

    @Test
    void testSelectFromTable_TwoORTerms_ShouldSelectSixTuples() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);

        // When
        SQLTerm[] sqlTerms = new SQLTerm[2];
        sqlTerms[0] = new SQLTerm(newTableName, id, ">", 5);
        sqlTerms[1] = new SQLTerm(newTableName, id, "<", 2);
        String[] strArrOperator = new String[] { "OR" };

        // Then
        Iterator it = engine.selectFromTable(sqlTerms, strArrOperator);
        assertEquals(6,getIteratorSize(it));
    }

    @Test
    void testSelectFromTable_InitialValueNotFound_ShouldSelectTenTuples() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);

        // When
        SQLTerm[] sqlTerms = new SQLTerm[1];
        sqlTerms[0] = new SQLTerm(newTableName, id, "<", 14);
        String[] strArrOperator = new String[] {};

        // Then
        Iterator it = engine.selectFromTable(sqlTerms, strArrOperator);
        assertEquals(10,getIteratorSize(it));
    }
    @Test
    void testSelectFromTable_OneANDTerm_InitialValuesNotFound_ShouldSelectSevenTuples() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);

        // When
        SQLTerm[] sqlTerms = new SQLTerm[2];
        sqlTerms[0] = new SQLTerm(newTableName, id, "<", 14);
        sqlTerms[1] = new SQLTerm(newTableName, id, ">=", 4);
        String[] strArrOperator = new String[] {"AND"};

        // Then
        Iterator it = engine.selectFromTable(sqlTerms, strArrOperator);
        assertEquals(7,getIteratorSize(it));
    }
    @Test
    void testSelectFromTable_LinearSearch_ShouldSelectOneTuple() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);

        // When
        SQLTerm[] sqlTerms = new SQLTerm[1];
        sqlTerms[0] = new SQLTerm(newTableName, name, "=", TEST_NAME);
        String[] strArrOperator = new String[] {};

        // Then
        Iterator it = engine.selectFromTable(sqlTerms, strArrOperator);
        assertEquals(10,getIteratorSize(it));
    }

    @Test
    void testSelectFromTable_TwoANDTerms_ShouldSelectZeroTuples() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);

        // When
        SQLTerm[] sqlTerms = new SQLTerm[2];
        sqlTerms[0] = new SQLTerm(newTableName, id, ">", 5);
        sqlTerms[1] = new SQLTerm(newTableName, id, "<", 2);
        String[] strArrOperator = new String[] { "AND" };

        // Then
        Iterator it = engine.selectFromTable(sqlTerms, strArrOperator);
        assertEquals(0, getIteratorSize(it));
    }

    @Test
    void testSelectWithIndex_ThreeANDTermsGreaterThan_ShouldSelectFiveTuples() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);
        engine.createIndex(newTableName, gpa, gpa+"Index");
        // When
        SQLTerm[] sqlTerms = new SQLTerm[3];
        sqlTerms[0] = new SQLTerm(newTableName, id, ">", 5);
        sqlTerms[1] = new SQLTerm(newTableName, name, "=", TEST_NAME);
        sqlTerms[2] = new SQLTerm(newTableName, gpa, "=", TEST_GPA);
        String[] strArrOperator = new String[] { "AND", "AND" };

        // Then
        Iterator it = engine.selectFromTable(sqlTerms, strArrOperator);
        assertEquals(5, getIteratorSize(it));
    }

    @Test
    void testSelectWithIndex_ONEANDTermsEqual_ShouldSelectZeroTuples() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);
        engine.createIndex(newTableName, id, id+"Index");
        // When
        SQLTerm[] sqlTerms = new SQLTerm[2];
        sqlTerms[0] = new SQLTerm(newTableName, id, "=", 5);
        sqlTerms[1] = new SQLTerm(newTableName, id, "=", 4);
        String[] strArrOperator = new String[] { "AND"};

        // Then
        Iterator it = engine.selectFromTable(sqlTerms, strArrOperator);
        assertEquals(0, getIteratorSize(it));
    }

    @Test
    void testSelectWithIndex_ONEORTermsEqual_ShouldSelectTwoTuples() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);
        engine.createIndex(newTableName, id, id+"Index");
        // When
        SQLTerm[] sqlTerms = new SQLTerm[2];
        sqlTerms[0] = new SQLTerm(newTableName, id, "=", 5);
        sqlTerms[1] = new SQLTerm(newTableName, id, "=", 4);
        String[] strArrOperator = new String[] { "OR"};

        // Then
        Iterator it = engine.selectFromTable(sqlTerms, strArrOperator);
        assertEquals(2, getIteratorSize(it));
    }

    @Test
    void testSelectWithIndex_Duplicates_ShouldSelectTwoTuples() throws DBAppException {
        // Given
        for (int i = 1; i <= 7; i++) {
            Hashtable<String, Object> htblColNameValue = createRow(i, TEST_NAME, TEST_GPA);
            engine.insertIntoTable(newTableName, htblColNameValue);
        }

        Hashtable<String, Object> htblColNameValue = createRow(11, "Farah", TEST_GPA);
        engine.insertIntoTable(newTableName, htblColNameValue);

        htblColNameValue = createRow(12, "Yara", TEST_GPA);
        engine.insertIntoTable(newTableName, htblColNameValue);

        engine.createIndex(newTableName, name, name+"Index");
        // When
        SQLTerm[] sqlTerms = new SQLTerm[1];
        sqlTerms[0] = new SQLTerm(newTableName, name, "=", TEST_NAME);
        String[] strArrOperator = new String[] {};

        // Then
        Iterator it = engine.selectFromTable(sqlTerms, strArrOperator);
        assertEquals(7, getIteratorSize(it));
    }

    @Test
    void testSelectWithIndex_ThreeANDTermsNotEqual_ShouldSelectNineTuples() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);
        engine.createIndex(newTableName, gpa, gpa+"Index");

        // When
        SQLTerm[] sqlTerms = new SQLTerm[3];
        sqlTerms[0] = new SQLTerm(newTableName, id, "!=", 5);
        sqlTerms[1] = new SQLTerm(newTableName, name, "=", TEST_NAME);
        sqlTerms[2] = new SQLTerm(newTableName, gpa, "=", TEST_GPA);
        String[] strArrOperator = new String[] { "AND", "AND" };

        // Then
        Iterator it = engine.selectFromTable(sqlTerms, strArrOperator);
        assertEquals(9, getIteratorSize(it));
    }

    @Test
    void testSelectWithIndex_ThreeANDTermsLessThanOrEqual_ShouldSelectSixTuples() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);
        engine.createIndex(newTableName, gpa, gpa+"Index");

        // When
        SQLTerm[] sqlTerms = new SQLTerm[3];
        sqlTerms[0] = new SQLTerm(newTableName, id, "<=", 6);
        sqlTerms[1] = new SQLTerm(newTableName, name, "=", TEST_NAME);
        sqlTerms[2] = new SQLTerm(newTableName, gpa, "=", TEST_GPA);
        String[] strArrOperator = new String[] { "AND", "AND" };

        // Then
        Iterator it = engine.selectFromTable(sqlTerms, strArrOperator);
        assertEquals(6,getIteratorSize(it));
    }

    @Test
    void testSelectWithIndex_FourTermsAndAtEnd_ShouldSelectFiveTuples() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);
        engine.createIndex(newTableName, gpa, gpa+"Index");

        // When
        SQLTerm[] sqlTerms = new SQLTerm[4];

        sqlTerms[0] = new SQLTerm(newTableName, id, "=", 5);
        sqlTerms[1] = new SQLTerm(newTableName, id, "<=", 6);
        sqlTerms[2] = new SQLTerm(newTableName, name, "=", TEST_NAME);
        sqlTerms[3] = new SQLTerm(newTableName, gpa, "=", TEST_GPA);
        String[] strArrOperator = new String[] { "XOR" ,"AND", "AND" };

        // Then
        Iterator it = engine.selectFromTable(sqlTerms, strArrOperator);
        assertEquals(5,getIteratorSize(it));
    }

    @Test
    void testSelectFromTable_TwoXORTerms_ShouldSelectFiveTuples() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);

        // When
        SQLTerm[] sqlTerms = new SQLTerm[2];
        sqlTerms[0] = new SQLTerm(newTableName, id, ">", 5);
        sqlTerms[1] = new SQLTerm(newTableName, name, "=", "yehia");
        String[] strArrOperator = new String[] { "XOR" };

        // Then
        Iterator it = engine.selectFromTable(sqlTerms, strArrOperator);
        assertEquals(5,getIteratorSize(it));
    }

    @Test
    void testSelectFromTable_WrongNumberOfOperators_ShouldFailSelection() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);

        // When
        SQLTerm[] sqlTerms = new SQLTerm[2];
        sqlTerms[0] = new SQLTerm(newTableName, id, ">", 5);
        sqlTerms[1] = new SQLTerm(newTableName, name, "=", "yehia");
        String[] strArrOperator = new String[] { "XOR", "AND" };

        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.selectFromTable(sqlTerms, strArrOperator);
        });

        // Then
        String expectedMessage = "Incompatible Array Lengths (arrSQLTerms should have a length bigger than strarrOperators by 1)";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage,outputMessage);
    }

    @Test
    void testSelectFromTable_UnknownArrOperator_ShouldFailSelection() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);

        // When
        SQLTerm[] sqlTerms = new SQLTerm[2];
        sqlTerms[0] = new SQLTerm(newTableName, id, ">", 5);
        sqlTerms[1] = new SQLTerm(newTableName, name, "=", "yehia");
        String[] strArrOperator = new String[] { "NOT" };

        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.selectFromTable(sqlTerms, strArrOperator);
        });

        // Then
        String expectedMessage = "Illegal operator";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage,outputMessage);
    }

    @Test
    void testSelectFromTable_UnknownOperator_ShouldFailSelection() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);

        // When
        SQLTerm[] sqlTerms = new SQLTerm[2];
        sqlTerms[0] = new SQLTerm(newTableName, id, ">", 5);
        sqlTerms[1] = new SQLTerm(newTableName, name, "<>", "yehia");
        String[] strArrOperator = new String[] { "AND" };

        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.selectFromTable(sqlTerms, strArrOperator);
        });

        // Then
        String expectedMessage = "Illegal operator";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage,outputMessage);
    }

    @Test
    void testSelectFromTable_InvalidColumn_ShouldFailSelection() throws DBAppException {
        // Given
        for (int i = 1; i <= 10; i++)
            insertRow(i);

        // When
        SQLTerm[] sqlTerms = new SQLTerm[2];
        sqlTerms[0] = new SQLTerm(newTableName, id, ">", 5);
        sqlTerms[1] = new SQLTerm(newTableName, "salary", "=", "yehia");
        String[] strArrOperator = new String[] { "AND" };

        Exception exception = assertThrows(DBAppException.class, () -> {
            engine.selectFromTable(sqlTerms, strArrOperator);
        });

        // Then
        String expectedMessage = "Column does not exist in table";
        String outputMessage = exception.getMessage();
        assertEquals(expectedMessage,outputMessage);
    }
    @Test
    void testDeleteFromTable_emptyHashTableShouldDeleteTableRows() throws DBAppException {
        for (int i = 1; i <= 10; i++)
            insertRow(i);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        engine.deleteFromTable(newTableName, htblColNameValue);
        Table table = FileManager.deserializeTable(newTableName);
        assertTrue(table.isEmpty());
    }
    private int getIteratorSize(Iterator it) {
        int ret = 0;
        while (it.hasNext()) {
            ret++;
            it.next();
        }
        return ret;
    }

    private static void insertRow(int id) throws DBAppException {

        Hashtable<String, Object> htblColNameValue = createRow(id, TEST_NAME, TEST_GPA);

        engine.insertIntoTable(newTableName, htblColNameValue);
    }
}
