package sqlservertockutil;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;


public class ssh {

    public static void main(String[] args) {
        String sqlServerUrl = "jdbc:sqlserver://smartpath10.tpddns.cn:2988;DatabaseName=TradingDouYin";
        //String sqlServerUrl = "jdbc:sqlserver://192.168.4.201:2422;DatabaseName=Taobao_trading";
        String sqlServerUsername = "CHH";
        String sqlServerPassword = "Y1v606";
        // ClickHouse数据库连接信息
        String clickHouseUrl = "jdbc:clickhouse://hadoop110:8123";
        String clickHouseUsername = "default";
        String clickHousePassword = "smartpath";
        String ckdatabase  = "dwd";
        String cktablename = "ceshi";

        String conf = "/opt/module/seatunnel-2.3.1/bin/start-seatunnel-spark-2-connector-v2.sh  \\\n" +
                "        --master yarn \\\n" +
                "        --deploy-mode client \\\n" +
                "        --config /opt/module/seatunnel-2.1.2/script_spark/clickhouse/ck/temp1.conf \\\n" +
                "        --variable url=\"" + sqlServerUrl + "\"\\\n" +
                "        --variable sqlserver_user=\"" + sqlServerUsername + "\"\\\n" +
                "        --variable sqlserver_password=\"" + sqlServerPassword + "\"\\\n" +
                "        --variable ck_database=\"" + ckdatabase + "\"\\\n" +
                "        --variable ck_table=\"" + cktablename + "\"";


        executeCommand(conf);

    }


    private static final Logger LOGGER = LogManager.getLogger(ssh.class);
    public static void executeCommand( String command ) {

        String host = "192.168.5.104";
        int port = 22;
        String username = "smartpath";
        String password = "cl@1109!@#";
        StringBuilder output = new StringBuilder();

        try {
            JSch jsch = new JSch();
            Session session = jsch.getSession(username, host, port);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();

            ChannelExec channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);

            InputStream commandOutput = channel.getInputStream();
            channel.setErrStream(System.err);

            channel.connect();

            BufferedReader reader = new BufferedReader(new InputStreamReader(commandOutput));
            String line;
            while (true) {
                line = reader.readLine();
                if (line == null) {
                    // 脚本执行完毕，退出循环
                    break;
                }
                LOGGER.info(line); // 实时输出日志信息
            }

            channel.disconnect();
            session.disconnect();
        } catch (Exception e) {
            LOGGER.error("Exception occurred:", e);
        }
    }



}
