package from_csv.csvtomysql;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

public class CsvToMySQL {
    private static final int BATCH_SIZE = 5000;
    private static final char DEFAULT_DELIMITER = ',';
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static final String COLUMN_TYPE = "VARCHAR(255)";

    public static void main(String[] args) {
        // 你提供的连接信息
       // String mysqlUrl = "jdbc:mysql://192.168.3.138:3306/mdlz?useSSL=false&serverTimezone=UTC";
        String mysqlUrl = "jdbc:mysql://192.168.5.110:3306/test?useSSL=false&serverTimezone=UTC";
        String mysqlUser = "root";
        //String mysqlPassword = "smartpthdata";
        String mysqlPassword = "smartpath";

        // 留位置给你填写
        String tableName = "foecoms_vds_rawdata_sep25_uga_rtgcoding"; // 例如：my_table
        Path csvPath = Paths.get("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\csvtomysql\\20251106FOeComsVDSRawData_Sep25-更新UGA-rtgcoding.csv"); // 例如：D:\\data\\sample.csv

        // 也支持从命令行传参：CsvToMySQL <csvPath> <tableName>
        if (args.length >= 2) {
            csvPath = Paths.get(args[0]);
            tableName = sanitizeName(args[1], true);
        }

        try {
            syncCsvToMySQL(csvPath, mysqlUrl, mysqlUser, mysqlPassword, tableName, DEFAULT_DELIMITER, DEFAULT_CHARSET);
            System.out.println("完成同步。");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void syncCsvToMySQL(Path csvFile, String jdbcUrl, String username, String password,
                                      String tableName, char delimiter, Charset charset) throws Exception {
        try (BufferedReader reader = Files.newBufferedReader(csvFile, charset)) {
            List<String> rawHeaders = readNextRecord(reader, delimiter);
            if (rawHeaders == null || rawHeaders.isEmpty()) {
                throw new IllegalArgumentException("CSV 文件为空或无表头: " + csvFile);
            }

            List<String> sanitizedHeaders = sanitizeHeaders(rawHeaders);
            boolean hasIdInCsv = hasIdRaw(rawHeaders);
            boolean addAutoId = !hasIdInCsv; // 如果 CSV 中没有 Id，则在表里加自增主键

            try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
                conn.setAutoCommit(false);
                createTableIfNotExists(conn, tableName, sanitizedHeaders, addAutoId);
                insertRows(conn, reader, tableName, sanitizedHeaders, delimiter, addAutoId);
                conn.commit();
            }
        }
    }

    private static void createTableIfNotExists(Connection conn, String tableName, List<String> columns, boolean addAutoId) throws SQLException {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ").append(backtick(sanitizeName(tableName, true))).append(" (");

        if (addAutoId) {
            ddl.append(backtick("Id")).append(" BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, ");
        }

        for (int i = 0; i < columns.size(); i++) {
            ddl.append(backtick(columns.get(i))).append(" ").append(COLUMN_TYPE);
            if (i < columns.size() - 1) ddl.append(", ");
        }
        ddl.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

        try (Statement st = conn.createStatement()) {
            st.execute(ddl.toString());
        }
    }

    private static void insertRows(Connection conn, BufferedReader reader, String tableName, List<String> columns,
                                   char delimiter, boolean addAutoId) throws Exception {
        // 如果表里人工加了 Id，自增列不参与 INSERT
        List<String> insertColumns = new ArrayList<>(columns);

        String colsJoined = String.join(", ", backtickAll(insertColumns));
        String placeholders = String.join(", ", Collections.nCopies(insertColumns.size(), "?"));
        String sql = "INSERT INTO " + backtick(sanitizeName(tableName, true)) + "(" + colsJoined + ") VALUES (" + placeholders + ")";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int count = 0;
            List<String> record;

            while ((record = readNextRecord(reader, delimiter)) != null) {
                // 对齐列数
                if (record.size() < insertColumns.size()) {
                    while (record.size() < insertColumns.size()) record.add("");
                } else if (record.size() > insertColumns.size()) {
                    record = record.subList(0, insertColumns.size());
                }

                for (int i = 0; i < insertColumns.size(); i++) {
                    String val = sanitizeValue(record.get(i));
                    ps.setString(i + 1, val);
                }

                ps.addBatch();
                count++;

                if (count % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    conn.commit();
                    System.out.println("已插入 " + count + " 行...");
                }
            }

            if (count % BATCH_SIZE != 0) {
                ps.executeBatch();
                conn.commit();
            }
            System.out.println("总插入行数: " + count);
        }
    }

    // 读取一个“记录”，支持跨行的带引号字段（例如字段中包含换行或逗号）
    private static List<String> readNextRecord(BufferedReader reader, char delimiter) throws IOException {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        boolean seenAnyLine = false;

        String line;
        while ((line = reader.readLine()) != null) {
            seenAnyLine = true;

            for (int i = 0; i < line.length(); i++) {
                char ch = line.charAt(i);
                if (inQuotes) {
                    if (ch == '"') {
                        // 解析转义引号 ""
                        if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                            current.append('"');
                            i++;
                        } else {
                            inQuotes = false;
                        }
                    } else {
                        current.append(ch);
                    }
                } else {
                    if (ch == '"') {
                        inQuotes = true;
                    } else if (ch == delimiter) {
                        tokens.add(current.toString());
                        current.setLength(0);
                    } else {
                        current.append(ch);
                    }
                }
            }

            // 行结束
            if (inQuotes) {
                // 字段内包含换行：保留一个换行，稍后在 sanitizeValue 中移除
                current.append('\n');
                continue; // 继续读下一行，直到引号闭合
            } else {
                break; // 本条记录结束
            }
        }

        if (!seenAnyLine) {
            return null; // EOF
        }

        // 收尾当前字段
        tokens.add(current.toString());
        return tokens;
    }

    private static String sanitizeValue(String val) {
        if (val == null) return "";
        // 去掉换行符
        val = val.replace("\r", "").replace("\n", "");
        return val;
    }

    private static boolean hasIdRaw(List<String> rawHeaders) {
        for (String h : rawHeaders) {
            if (h != null && h.trim().equalsIgnoreCase("id")) {
                return true;
            }
        }
        return false;
    }

    private static List<String> sanitizeHeaders(List<String> headers) {
        List<String> result = new ArrayList<>();
        Map<String, Integer> seen = new HashMap<>();
        for (int idx = 0; idx < headers.size(); idx++) {
            String h = headers.get(idx);
            String base = sanitizeName(h, false);
            if (base.isEmpty()) base = "col_" + (idx + 1);
            int count = seen.getOrDefault(base, 0);
            if (count > 0) {
                String candidate = base + "_" + count;
                seen.put(base, count + 1);
                result.add(candidate);
            } else {
                seen.put(base, 1);
                result.add(base);
            }
        }
        return result;
    }

    private static String sanitizeName(String name, boolean forTable) {
        if (name == null) return "";
        String s = name.trim();
        s = s.replaceAll("[^a-zA-Z0-9_]", "_");
        s = s.replaceAll("_+", "_");
        s = s.replaceAll("^_+", "");
        s = s.replaceAll("_+$", "");
        if (s.isEmpty()) return "";
        if (Character.isDigit(s.charAt(0))) s = (forTable ? "t_" : "c_") + s;
        return s;
    }

    private static String backtick(String identifier) {
        return "`" + identifier + "`";
    }

    private static List<String> backtickAll(List<String> ids) {
        List<String> r = new ArrayList<>();
        for (String id : ids) r.add(backtick(id));
        return r;
    }
}