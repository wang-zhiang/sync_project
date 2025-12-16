


package from_csv;

        import java.sql.Connection;
        import java.sql.DriverManager;
        import java.sql.Statement;

public class delete_all_node_table {
    public static void main(String[] args) {
        String[] nodes = {"hadoop104", "hadoop105", "hadoop106", "hadoop107", "hadoop108", "hadoop109", "hadoop110"};
        String tableName = "test.ddddshoprelnewbeer_ss_dict_assist";
        String clickHouseUrlTemplate = "jdbc:clickhouse://%s:8123?user=default&password=smartpath";

        for (String node : nodes) {
            String url = String.format(clickHouseUrlTemplate, node);
            System.out.println("Connecting to node: " + node);

            try (Connection connection = DriverManager.getConnection(url);
                 Statement statement = connection.createStatement()) {

                String dropTableQuery = "DROP TABLE IF EXISTS " + tableName;
                statement.execute(dropTableQuery);
                System.out.println("Table " + tableName + " deleted successfully on node: " + node);
            } catch (Exception e) {
                System.err.println("Failed to delete table " + tableName + " on node: " + node);
                e.printStackTrace();
            }
        }
    }
}
