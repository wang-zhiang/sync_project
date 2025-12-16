//package Flink_demo;
//
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//
//public class ceshi2 {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 构建 SqlServerSource
//        SqlServerSource source = SqlServerSource.builder()
//                .hostname("192.168.4.218")
//                .port(2599)  // 注意端口应为 1433（2599 是非标准端口，需确认 SQL Server 配置）
//                .database("datasystem")
//                .username("CHH")
//                .password("Y1v606")
//                .tableList("dbo.skuall_ceshi")
//                .deserializer(new JsonDebeziumDeserializationSchema<String>())
//                .build();
//
//        // 使用 addSource 替代 fromSource
//        env.addSource(source)
//                .print();
//
//        env.execute("Flink CDC SQL Server Example");
//    }
//}