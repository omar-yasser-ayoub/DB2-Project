# JavaDB

JavaDB is a lightweight database application written in Java. It supports basic SQL operations such as creating tables, inserting, updating, and deleting rows, and creating indices. It also features a simple SQL parser using ANTLR for query execution.

## Features

- Create tables with specified columns and types (String, Integer, or Double)
- Insert rows into tables
- Update existing rows
- Delete rows based on conditions
- Create indices on columns (B+ Trees)
- Execute SQL queries via a custom SQL parser

## Usage
### Initialization
To start using JavaDB, create an instance of the DBApp class. This will initialize the database with necessary configurations.
```java
DBApp dbApp = new DBApp();
```

### Create a Table
Use the createTable method to create a new table.
```java
Hashtable<String, String> columns = new Hashtable<>();
columns.put("id", "java.lang.Integer");
columns.put("name", "java.lang.String");
columns.put("gpa", "java.lang.Double");

dbApp.createTable("Student", "id", columns);
```

### Insert into Table
Use the insertIntoTable method to insert a new row into a table.
```java
Hashtable<String, Object> row = new Hashtable<>();
row.put("id", 1);
row.put("name", "John Doe");
row.put("gpa", 3.5);

dbApp.insertIntoTable("Student", row);
```

### Update Table
Use the updateTable method to update existing rows in a table.
```java
Hashtable<String, Object> updatedValues = new Hashtable<>();
updatedValues.put("name", "John Smith");
updatedValues.put("gpa", 3.8);

dbApp.updateTable("Student", "1", updatedValues);
```

### Delete from Table
Use the deleteFromTable method to delete rows from a table.
```java
Hashtable<String, Object> conditions = new Hashtable<>();
conditions.put("id", 1);

dbApp.deleteFromTable("Student", conditions);
```

### Create Index
Use the createIndex method to create an index on a column.
```java
dbApp.createIndex("Student", "name", "nameIndex");
```

### Execute SQL Queries
Use the parseSQL method to execute SQL queries.
```java
StringBuffer sqlQuery = new StringBuffer("SELECT * FROM Student WHERE gpa > 3.0;");
Iterator<Tuple> results = dbApp.parseSQL(sqlQuery);

while (results.hasNext()) {
    System.out.println(results.next());
}
```
### Configuration
Configuration for the maximum number of rows in a page is set in the `DBApp.config file` located in the `src/main/java/org/example/resources` directory.
```java
MaximumRowsCountinPage=200

```
