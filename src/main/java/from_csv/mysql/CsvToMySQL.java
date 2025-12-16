package from_csv.mysql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.util.*;

public class CsvToMySQL {
    // Database credentials
    private static final String DB_URL = "jdbc:mysql://192.168.6.101:3306/mdlz";
    private static final String USER = "root";
    private static final String PASSWORD = "smartpath";

    public static void main(String[] args) throws Exception {
        String csvFilePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\csv_file\\source4_ldl_other\\20250924 LSL cake & Biscuit Data (2301-2508).csv";
        String tableName = "yp";  // Change this to the table name in MySQL
        boolean autoCreateTable = true; // Set to true to auto-create table
        boolean insertIntoExistingTable = false; // Set to true to insert into an existing table

        List<String> columns = new ArrayList<>(); // If you want to specify columns, you can list them here

        readCsvAndInsertIntoMySQL(csvFilePath, tableName, autoCreateTable, insertIntoExistingTable, columns);
    }

    public static void readCsvAndInsertIntoMySQL(String csvFilePath, String tableName, boolean autoCreateTable, boolean insertIntoExistingTable, List<String> columns) throws Exception {
        // Step 1: Read CSV File
        BufferedReader reader = new BufferedReader(new FileReader(new File(csvFilePath)));
        String line = null;
        List<Map<String, String>> dataList = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();

        // Read the header row (first row) to get column names
        if ((line = reader.readLine()) != null) {
            columnNames = Arrays.asList(line.split(","));
        }

        // Read the data rows
        while ((line = reader.readLine()) != null) {
            String[] values = line.split(",");
            Map<String, String> dataMap = new HashMap<>();
            for (int i = 0; i < columnNames.size(); i++) {
                dataMap.put(columnNames.get(i), values[i]);
            }
            dataList.add(dataMap);
        }

        reader.close();

        // Step 2: Connect to MySQL and process
        Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);

        // If autoCreateTable is true, create table if it doesn't exist
        if (autoCreateTable) {
            createTableIfNotExists(conn, tableName, columnNames);
        }

        // Step 3: Insert data
        if (insertIntoExistingTable) {
            insertIntoExistingTable(conn, tableName, columnNames, dataList);
        } else {
            insertIntoNewTable(conn, tableName, columnNames, dataList);
        }

        conn.close();
    }

    private static void createTableIfNotExists(Connection conn, String tableName, List<String> columnNames) throws SQLException {
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");

        for (String column : columnNames) {
            createTableQuery.append(column).append(" NVARCHAR(500), ");
        }
        createTableQuery.delete(createTableQuery.length() - 2, createTableQuery.length());
        createTableQuery.append(")");

        Statement stmt = conn.createStatement();
        stmt.executeUpdate(createTableQuery.toString());
    }

    private static void insertIntoExistingTable(Connection conn, String tableName, List<String> columnNames, List<Map<String, String>> dataList) throws SQLException {
        String insertSQL = buildInsertSQL(tableName, columnNames);
        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            conn.setAutoCommit(false);

            for (Map<String, String> data : dataList) {
                int index = 1;
                for (String column : columnNames) {
                    pstmt.setString(index++, data.getOrDefault(column, ""));
                }
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            conn.commit();
        }
    }

    private static void insertIntoNewTable(Connection conn, String tableName, List<String> columnNames, List<Map<String, String>> dataList) throws SQLException {
        String insertSQL = buildInsertSQL(tableName, columnNames);
        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            conn.setAutoCommit(false);

            for (Map<String, String> data : dataList) {
                int index = 1;
                for (String column : columnNames) {
                    pstmt.setString(index++, data.getOrDefault(column, ""));
                }
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            conn.commit();
        }
    }

    private static String buildInsertSQL(String tableName, List<String> columnNames) {
        StringBuilder insertSQL = new StringBuilder("INSERT INTO " + tableName + " (");
        for (String column : columnNames) {
            insertSQL.append(column).append(", ");
        }
        insertSQL.delete(insertSQL.length() - 2, insertSQL.length());
        insertSQL.append(") VALUES (");

        for (int i = 0; i < columnNames.size(); i++) {
            insertSQL.append("?, ");
        }
        insertSQL.delete(insertSQL.length() - 2, insertSQL.length());
        insertSQL.append(")");

        return insertSQL.toString();
    }
}
