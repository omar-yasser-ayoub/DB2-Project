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
import org.example.data_structures.SQLTerm;
import org.example.data_structures.Tuple;
import org.example.exceptions.DBAppException;

import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import static org.example.DBApp.METADATA_DIR;

public class ANTLRManager {

    public static DBApp dbApp;
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

    public static Iterator<Tuple> callMethod(Vector<String> tokens) throws DBAppException {
        if(tokens.get(0).equalsIgnoreCase("CREATE")) {
            if(tokens.get(1).equalsIgnoreCase("TABLE")) {
                return antlrCreateTable(tokens);
            }
            else if (tokens.get(1).equalsIgnoreCase("INDEX")) {
                return antlrCreateIndex(tokens);
            }
            else {
                throw new DBAppException("Unsupported SQL statement");
            }
        }
        else if(tokens.get(0).equalsIgnoreCase("INSERT")) {
            if(tokens.get(1).equalsIgnoreCase("INTO")) {
                if(tokens.get(3).equalsIgnoreCase("VALUES"))
                    return antlrInsert(tokens);
                else
                    return antlrInsertSpecific(tokens);
            }
            else {
                throw new DBAppException("Unsupported SQL statement");
            }
        }
        else if(tokens.get(0).equalsIgnoreCase("DELETE")) {
            if(tokens.get(1).equalsIgnoreCase("FROM")) {
                if(tokens.size() <= 4) {
                    return antlrDeleteAll(tokens);
                }
                else if(tokens.get(3).equalsIgnoreCase("WHERE")) {
                    return antlrDelete(tokens);
                }
                else {
                    throw new DBAppException("Unsupported SQL statement");
                }
            }
            else {
                throw new DBAppException("Unsupported SQL statement");
            }
        }
        else if(tokens.get(0).equalsIgnoreCase("SELECT")) {
            if(tokens.get(1).equals("*") && tokens.size() > 5) {
                if(tokens.get(4).equalsIgnoreCase("WHERE")) {
                    return antlrSelect(tokens);
                }
                else {
                    throw new DBAppException("Unsupported SQL statement");
                }
            }
            else {
                throw new DBAppException("Unsupported SQL statement");
            }
        }
        else if(tokens.get(0).equalsIgnoreCase("UPDATE")) {
            if(tokens.get(2).equalsIgnoreCase("SET") && containsIgnoreCase(tokens, "WHERE")) {
                return antlrUpdate(tokens);
            }
            else {
                throw new DBAppException("Unsupported SQL statement");
            }
        }
        else {
            throw new DBAppException("Unsupported SQL statement");
        }
    }

    private static boolean containsIgnoreCase(Vector<String> tokens, String s) {
        Vector<String> uppercase = new Vector<>();
        for(String token : tokens) {
            uppercase.add(token.toUpperCase());
        }
        return uppercase.contains(s.toUpperCase());
    }

    private static Iterator<Tuple> antlrCreateTable(Vector<String> tokens) throws DBAppException {
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
        dbApp.createTable(tableName, primaryKey, colNameType);
        return null;
    }

    private static Iterator<Tuple> antlrCreateIndex(Vector<String> tokens) throws DBAppException {
        String indexName = tokens.get(2);
        String tableName = tokens.get(4);
        String colName = tokens.get(6);
        if(!tokens.get(7).equals(")"))
            throw new DBAppException("Unsupported SQL statement (index on multiple columns)");
        if(tokens.size() > 9) {
            if(!tokens.get(9).equalsIgnoreCase("BTREE"))
                throw new DBAppException("Unsupported SQL statement (index of type '" + tokens.get(9) + "')");
        }
        dbApp.createIndex(tableName, colName, indexName);
        return null;
    }

    private static Iterator<Tuple> antlrInsert(Vector<String> tokens) throws DBAppException {
        String tableName = tokens.get(2);
        Vector<String> colNames = new Vector<>();
        Vector<String> colTypes = new Vector<>();
        try {
            CSVReader reader = new CSVReader(new FileReader(METADATA_DIR + "/metadata.csv"));
            String[] line = reader.readNext();

            while ((line = reader.readNext()) != null) {
                if(line[0].equals(tableName)) {
                    colNames.add(line[1]);
                    colTypes.add(line[2]);
                }
            }
        } catch (CsvValidationException | IOException e) {
            throw new DBAppException(e.getMessage());
        }

        Hashtable<String, Object> colNameValue = new Hashtable<>();
        int i = 5;
        int j = 0;
        while(j < colNames.size() && i < tokens.size()) {
            String val = tokens.get(i);
            String type = colTypes.get(j);

            try {
                switch (type) {
                    case "java.lang.Integer":
                        colNameValue.put(colNames.get(j), Integer.valueOf(Integer.parseInt(val)));
                        break;
                    case "java.lang.Double":
                        colNameValue.put(colNames.get(j), Double.valueOf(Double.parseDouble(val)));
                        break;
                    case "java.lang.String":
                        if(val.charAt(0) == '\'')
                            val = val.substring(1, val.length() - 1);
                        colNameValue.put(colNames.get(j), val);
                        break;
                }
            } catch (NumberFormatException e) {
                throw new DBAppException("Mismatching datatypes");
            }
            j++;
            i += 2;
            if (tokens.get(i).equals(";"))
                break;
        }
        dbApp.insertIntoTable(tableName, colNameValue);
        return null;
    }

    private static Iterator<Tuple> antlrInsertSpecific(Vector<String> tokens) throws DBAppException {
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
        while(j < colNames.size() && i < tokens.size()) {
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
                        if(val.charAt(0) == '\'')
                            val = val.substring(1, val.length() - 1);
                        colNameValue.put(colNames.get(j), val);
                        break;
                }
            } catch (NumberFormatException e) {
                throw new DBAppException("Mismatching datatypes");
            }
            j++;
            i += 2;
            if (tokens.get(i).equals(";"))
                break;
        }
        dbApp.insertIntoTable(tableName, colNameValue);
        return null;
    }

    private static Iterator<Tuple> antlrDeleteAll(Vector<String> tokens) throws DBAppException {
        dbApp.deleteFromTable(tokens.get(2), new Hashtable<String, Object>());
        return null;
    }

    private static Iterator<Tuple> antlrDelete(Vector<String> tokens) throws DBAppException {
        String tableName = tokens.get(2);

        Hashtable<String, String> colNameType = new Hashtable<>();
        Vector<String> colNames = new Vector<>();
        Vector<String> vals = new Vector<>();
        for(int i = 4; i < tokens.size(); i++) {
            colNames.add(tokens.get(i));
            colNameType.put(tokens.get(i), "");
            if(!tokens.get(i + 1).equals("="))
                throw new DBAppException("Unsupported SQL statement");
            vals.add(tokens.get(i + 2));
            i += 3;
            if(i >= tokens.size() || tokens.get(i).equals(";"))
                break;
            else if(!tokens.get(i).equalsIgnoreCase("AND"))
                throw new DBAppException("Unsupported SQL statement");
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
        for(int i = 0; i < colNames.size(); i++) {
            String val = vals.get(i);
            String type = colNameType.get(colNames.get(i));

            try {
                switch (type) {
                    case "java.lang.Integer":
                        colNameValue.put(colNames.get(i), Integer.valueOf(Integer.parseInt(val)));
                        break;
                    case "java.lang.Double":
                        colNameValue.put(colNames.get(i), Double.valueOf(Double.parseDouble(val)));
                        break;
                    case "java.lang.String":
                        if(val.charAt(0) == '\'')
                            val = val.substring(1, val.length() - 1);
                        colNameValue.put(colNames.get(i), val);
                        break;
                }
            } catch (NumberFormatException e) {
                throw new DBAppException("Mismatching datatypes");
            }
        }

        dbApp.deleteFromTable(tableName, colNameValue);
        return null;
    }

    private static Iterator<Tuple> antlrSelect(Vector<String> tokens) throws DBAppException {
        String tableName = tokens.get(3);
        Vector<String> colNames = new Vector<>();
        Vector<String> compOps = new Vector<>();
        Vector<String> logOps = new Vector<>();
        Vector<String> vals = new Vector<>();
        Hashtable<String, String> colNameType = new Hashtable<>();

        for(int i = 5; i < tokens.size(); i++) {
            if(tokens.get(i).equals(";"))
                break;

            colNames.add(tokens.get(i));
            colNameType.put(tokens.get(i), "");
            i++;

            if(tokens.get(i).equals(">") || tokens.get(i).equals("<") || tokens.get(i).equals("!")) {
                if(tokens.get(i + 1).equals("=")) {
                    compOps.add(tokens.get(i) + "=");
                    i += 2;
                }
                else {
                    compOps.add(tokens.get(i));
                    i++;
                }
            }
            else {
                compOps.add(tokens.get(i));
                i++;
            }

            vals.add(tokens.get(i));
            i++;

            if(i >= tokens.size() || tokens.get(i).equals(";"))
                break;

            if(tokens.get(i).equals("AND") || tokens.get(i).equals("OR") || tokens.get(i).equals("XOR")) {
                logOps.add(tokens.get(i));
            }
            else {
                throw new DBAppException("Unsupported SQL statement");
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

        Vector<Object> colValues = new Vector<>();
        for(int i = 0; i < colNames.size(); i++) {
            String val = vals.get(i);
            String type = colNameType.get(colNames.get(i));

            try {
                switch (type) {
                    case "java.lang.Integer":
                        colValues.add(Integer.valueOf(Integer.parseInt(val)));
                        break;
                    case "java.lang.Double":
                        colValues.add(Double.valueOf(Double.parseDouble(val)));
                        break;
                    case "java.lang.String":
                        if(val.charAt(0) == '\'')
                            val = val.substring(1, val.length() - 1);
                        colValues.add(val);
                        break;
                }
            } catch (NumberFormatException e) {
                throw new DBAppException("Mismatching datatypes");
            }
        }

        SQLTerm[] SQLTerms = new SQLTerm[colNames.size()];
        String[] operators = new String[colNames.size() - 1];
        for(int i = 0; i < SQLTerms.length; i++) {
            SQLTerms[i] = new SQLTerm(tableName, colNames.get(i), compOps.get(i), colValues.get(i));
        }
        for(int i = 0; i < operators.length; i++) {
            operators[i] = logOps.get(i);
        }

        return dbApp.selectFromTable(SQLTerms, operators);
    }

    private static Iterator<Tuple> antlrUpdate(Vector<String> tokens) throws DBAppException {
        String tableName = tokens.get(1);
        Vector<String> colNames = new Vector<>();
        Vector<String> vals = new Vector<>();
        Hashtable<String, String> colNameType = new Hashtable<>();

        int i;
        for(i = 3; i < tokens.size(); i++) {
            colNames.add(tokens.get(i));
            colNameType.put(tokens.get(i), "");
            i += 2;
            vals.add(tokens.get(i));
            i++;
            if(tokens.get(i).equalsIgnoreCase("WHERE"))
                break;
        }

        String clusteringKey = "";
        String clusteringKeyType = "";
        try {
            CSVReader reader = new CSVReader(new FileReader(METADATA_DIR + "/metadata.csv"));
            String[] line = reader.readNext();

            while ((line = reader.readNext()) != null) {
                if(line[0].equals(tableName)) {
                    if (colNameType.containsKey(line[1])) {
                        colNameType.put(line[1], line[2]);
                    }
                    if(line[3].equals("True")) {
                        clusteringKey = line[1];
                        clusteringKeyType = line[2];
                    }
                }
            }
        } catch (CsvValidationException | IOException e) {
            throw new DBAppException(e.getMessage());
        }

        i++;
        if(!tokens.get(i).equals(clusteringKey)) {
            throw new DBAppException("Unsupported SQL statement");
        }
        i++;
        if(!tokens.get(i).equals("=")) {
            throw new DBAppException("Unsupported SQL statement");
        }
        i++;
        String clusteringKeyValue = tokens.get(i);;
        if(tokens.get(i).charAt(0) == '\'' && clusteringKeyType.equals("java.lang.String")) {
            clusteringKeyValue = clusteringKeyValue.substring(1, clusteringKeyValue.length() - 1);
        }
        i++;
        if(i < tokens.size() && !tokens.get(i).equals(";")) {
            throw new DBAppException("Unsupported SQL statement");
        }

        Hashtable<String, Object> colNameValue = new Hashtable<>();
        for(int j = 0; j < colNames.size(); j++) {
            String val = vals.get(j);
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
                        if(val.charAt(0) == '\'')
                            val = val.substring(1, val.length() - 1);
                        colNameValue.put(colNames.get(j), val);
                        break;
                }
            } catch (NumberFormatException e) {
                throw new DBAppException("Mismatching datatypes");
            }
        }

        dbApp.updateTable(tableName, clusteringKeyValue, colNameValue);
        return null;
    }
}
