package org.example.managers;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.example.ANTLR.MySqlLexer;
import org.example.ANTLR.MySqlParser;
import org.example.DBApp;
import org.example.data_structures.Page;
import org.example.exceptions.DBAppException;

import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Vector;

import static org.example.DBApp.METADATA_DIR;

public class ANTLRManager {
    private ANTLRManager() {
        throw new IllegalStateException("Utility class");
    }

    public static void checkValidSQL(StringBuffer strbufSQL) throws DBAppException {
        MySqlLexer lexer = new MySqlLexer(CharStreams.fromString(String.valueOf(strbufSQL)));
        MySqlParser parser = new MySqlParser(new CommonTokenStream(lexer));
        MySqlParser.RootContext context = parser.root();
        if(parser.getNumberOfSyntaxErrors() != 0 || String.valueOf(strbufSQL).equals(""))
            throw new DBAppException("Invalid SQL statement");
    }

    public static Vector<String> getTokenStrings(StringBuffer strbufSQL) {
        MySqlLexer lexer = new MySqlLexer(CharStreams.fromString(String.valueOf(strbufSQL)));
        CommonTokenStream ts = new CommonTokenStream(lexer);
        ts.fill();
        Vector<Token> tokens = new Vector<>(ts.getTokens());
        Vector<String> tokenStrings = new Vector<>();
        for(Token t : tokens) {
            if(!t.getText().equals(" ") && !t.getText().equals("<EOF>") && !t.getText().equals("\n"))
                tokenStrings.add(t.getText());
        }

        return tokenStrings;
    }

    public static void callMethod(Vector<String> tokens) throws DBAppException {
        if(tokens.get(0).equalsIgnoreCase("CREATE")) {
            if(tokens.get(1).equalsIgnoreCase("TABLE")) {
                antlrCreateTable(tokens);
            }
            else if (tokens.get(1).equalsIgnoreCase("INDEX")) {
                antlrCreateIndex(tokens);
            }
            else {
                throw new DBAppException("Unsupported SQL statement");
            }
        }
        else if(tokens.get(0).equalsIgnoreCase("INSERT")) {
            if(tokens.get(1).equalsIgnoreCase("INTO")) {
                if(tokens.get(3).equalsIgnoreCase("VALUES"))
                    antlrInsert(tokens);
                else
                    antlrInsertSpecific(tokens);
            }
            else {
                throw new DBAppException("Unsupported SQL statement");
            }
        }
        else {
            throw new DBAppException("Unsupported SQL statement");
        }
    }

    public static void antlrCreateTable(Vector<String> tokens) throws DBAppException {
        String tableName = tokens.get(2);
        Hashtable<String,String> colNameType = new Hashtable<>();
        String primaryKey = null;
        String currColumn = null;
        for(int i = 4; i < tokens.size(); i++) {
            if(tokens.get(i).equals(";"))
                break;

            if(tokens.get(i).equalsIgnoreCase("PRIMARY")) {
                if(tokens.get(i + 1).equalsIgnoreCase("KEY")) {
                    if(tokens.get(i + 4).equalsIgnoreCase(","))
                        throw new DBAppException("Unsupported SQL statement (multiple primary keys)");
                    primaryKey = tokens.get(i + 3);
                    i += 5;
                    if(tokens.get(i).equals(")"))
                        break;
                }
            }

            // add column name and type then increment i
            String type = switch (tokens.get(i + 1).toUpperCase()) {
                case "VARCHAR", "CHAR" -> "java.lang.String";
                case "INT", "INTEGER" -> "java.lang.Integer";
                case "DOUBLE" -> "java.lang.Double";
                default -> throw new DBAppException("Unsupported SQL statement (unsupported datatype '" + tokens.get(i + 1) + "')");
            };
            colNameType.put(tokens.get(i), type);
            currColumn = tokens.get(i);
            i += 2;

            // skip over any size limits on datatypes
            if(tokens.get(i).equals("(")) {
                i += 3;
            }

            // check if this column is the primary key
            if(tokens.get(i).equalsIgnoreCase("PRIMARY")) {
                if(tokens.get(i + 1).equalsIgnoreCase("KEY")) {
                    if(primaryKey == null)
                        primaryKey = currColumn;
                    else
                        throw new DBAppException("Unsupported SQL statement (multiple primary keys)");
                    i += 2;
                }
            }
            else if(!tokens.get(i).equals(",") && !tokens.get(i).equals(")")) {
                throw new DBAppException("Unsupported SQL statement");
            }
        }

        if(primaryKey == null) {
            throw new DBAppException("Table must have a primary key");
        }

        DBApp dbApp = new DBApp();
        dbApp.createTable(tableName, primaryKey, colNameType);
    }

    public static void antlrCreateIndex(Vector<String> tokens) throws DBAppException {
        String indexName = tokens.get(2);
        String tableName = tokens.get(4);
        String colName = tokens.get(6);
        if(!tokens.get(7).equals(")"))
            throw new DBAppException("Unsupported SQL statement (index on multiple columns)");
        if(tokens.size() > 9) {
            if(!tokens.get(9).equalsIgnoreCase("BTREE"))
                throw new DBAppException("Unsupported SQL statement (index of type '" + tokens.get(9) + "')");
        }

        DBApp dbApp = new DBApp();
        dbApp.createIndex(tableName, colName, indexName);
    }

    public static void antlrInsert(Vector<String> tokens) throws DBAppException {

    }

    public static void antlrInsertSpecific(Vector<String> tokens) throws DBAppException {
        String tableName = tokens.get(2);
        Hashtable<String, String> colNameType = new Hashtable<>();
        Vector<String> colNames = new Vector<>();
        int i;
        for(i = 4; i < tokens.size(); i++) {
            colNameType.put(tokens.get(i), "");
            colNames.add(tokens.get(i));
            i++;
            if(tokens.get(i).equals(")")) {
                i += 3;
                break;
            }
        }

        try {
            CSVReader reader = new CSVReader(new FileReader(METADATA_DIR + "/metadata.csv"));
            String[] line = reader.readNext();

            while ((line = reader.readNext()) != null) {
                if(line[0].equals(tableName)) {
                    if (colNameType.containsKey(line[1])) {
                        colNameType.put(line[1], line[2]);
                    }
                }
            }
        } catch (CsvValidationException | IOException e) {
            throw new DBAppException(e.getMessage());
        }

        Hashtable<String, Object> colNameValue = new Hashtable<>();
        int j = 0;
        while(j < colNames.size()) {
            String val = tokens.get(i);
            String type = colNameType.get(colNames.get(j));

            try {
                switch (type) {
                    case "java.lang.Integer":
                        colNameValue.put(colNames.get(j), Integer.valueOf(Integer.parseInt(val)));
                        break;
                    case "java.lang.Double":
                        colNameValue.put(colNames.get(j), Double.valueOf(Double.parseDouble(val)));
                        break;
                    case "java.lang.String":
                        colNameValue.put(colNames.get(j), val);
                        break;
                }
            } catch (NumberFormatException e) {
                throw new DBAppException("Mismatching datatypes");
            }
            j++;
            i += 2;
        }

//        for(int k = 0; k < colNameValue.size(); k++){
//            System.out.println(colNames.get(i) + " " + colNameValue.get(colNames.get(i)));
//        }

        DBApp dbApp = new DBApp();
        dbApp.insertIntoTable(tableName, colNameValue);
    }
}
