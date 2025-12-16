package dim;
import java.util.List;
import java.util.Map;

public class TaskConfig {
    private ClickHouseConfig clickhouse;
    private List<Task> tasks;
    
    // Getters and Setters
    public ClickHouseConfig getClickhouse() { return clickhouse; }
    public void setClickhouse(ClickHouseConfig clickhouse) { this.clickhouse = clickhouse; }
    public List<Task> getTasks() { return tasks; }
    public void setTasks(List<Task> tasks) { this.tasks = tasks; }
    
    public static class ClickHouseConfig {
        private String host;
        private int port;
        private String database;
        private String username;
        private String password;
        
        // Getters and Setters
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getDatabase() { return database; }
        public void setDatabase(String database) { this.database = database; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
    }
    
    public static class Task {
        private String taskId;
        private Source source;
        private String targetTable;
        private int tableType;
        private List<String> primaryKeys;
        private List<FieldMapping> fieldMappings;
        
        // Getters and Setters
        public String getTaskId() { return taskId; }
        public void setTaskId(String taskId) { this.taskId = taskId; }
        public Source getSource() { return source; }
        public void setSource(Source source) { this.source = source; }
        public String getTargetTable() { return targetTable; }
        public void setTargetTable(String targetTable) { this.targetTable = targetTable; }
        public int getTableType() { return tableType; }
        public void setTableType(int tableType) { this.tableType = tableType; }
        public List<String> getPrimaryKeys() { return primaryKeys; }
        public void setPrimaryKeys(List<String> primaryKeys) { this.primaryKeys = primaryKeys; }
        public List<FieldMapping> getFieldMappings() { return fieldMappings; }
        public void setFieldMappings(List<FieldMapping> fieldMappings) { this.fieldMappings = fieldMappings; }
    }
    
    public static class Source {
        private String host;
        private int port;
        private String database;
        private String table;
        private String user;
        private String password;
        
        // Getters and Setters
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getDatabase() { return database; }
        public void setDatabase(String database) { this.database = database; }
        public String getTable() { return table; }
        public void setTable(String table) { this.table = table; }
        public String getUser() { return user; }
        public void setUser(String user) { this.user = user; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
    }
    
    public static class FieldMapping {
        private String sourceField;
        private String targetField;
        
        // Getters and Setters
        public String getSourceField() { return sourceField; }
        public void setSourceField(String sourceField) { this.sourceField = sourceField; }
        public String getTargetField() { return targetField; }
        public void setTargetField(String targetField) { this.targetField = targetField; }
    }
}