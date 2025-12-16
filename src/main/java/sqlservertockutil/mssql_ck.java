package sqlservertockutil;
/*

支持sqlserver同步数据到ck
ck最好是新建表，插入数据有点麻烦，要提前删除覆盖的语句，而且这里的字段都是经过拼音转化的，不一定能对应的上

 * */

import sqlservertockutil.chinesetopinyin;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


/*
自用的工具，和本项目无关
* */

public class mssql_ck {

    public static void main(String[] args) {
        //定义sqlserver表名，ck表名 ,ck库名， sqlserver url ，密码 ，用户等信息 即可完成在clickhouse上面建表

        //clickhouse要指定库名以及表名，不然就是在默认里


        String  cktable = "test.a";
        String sqltable ="brandgenxing";




        // SQL Server数据库连接信息
       // String sqlServerUrl = "jdbc:sqlserver://smartpath10.tpddns.cn:2988;DatabaseName=TradingDouYin";
        //String sqlServerUrl = "jdbc:sqlserver://192.168.4.201:2422;DatabaseName=Taobao_trading";
        String sqlServerUrl = "jdbc:sqlserver://192.168.4.218:2599;DatabaseName=datasystem";
        //String sqlServerUrl = "jdbc:sqlserver://192.168.4.212:2533;DatabaseName=websearchc";
        String sqlServerUsername = "CHH";
        String sqlServerPassword = "Y1v606";

        //String sqlServerUrl = "jdbc:sqlserver://192.168.3.183;DatabaseName=SourceDate";
       // String sqlServerUrl = "jdbc:sqlserver://192.168.4.36;DatabaseName=WebSearchC";
       // String sqlServerUrl = "jdbc:sqlserver://192.168.4.218:2599;DatabaseName=datasystem";
//        String sqlServerUsername = "sa";
//        String sqlServerPassword = "smartpthdata";

        // ClickHouse数据库连接信息
        String clickHouseUrl = "jdbc:clickhouse://hadoop110:8123";
        String clickHouseUsername = "default";
        String clickHousePassword = "smartpath";




        Connection sqlServerConnection = null;
        Connection clickHouseConnection = null;
        Statement sqlServerStatement = null;
        ResultSet sqlServerResultSet = null;

        try {
            // 连接SQL Server数据库
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            sqlServerConnection = DriverManager.getConnection(sqlServerUrl, sqlServerUsername, sqlServerPassword);

            // 执行SQL查询，读取表的字段和数据类型
            String sqlQuery = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = " + "'" + sqltable + "'" ;
            System.out.println("sql查询语句：" + sqlQuery);
            sqlServerStatement = sqlServerConnection.createStatement();
            sqlServerResultSet = sqlServerStatement.executeQuery(sqlQuery);

//            // 连接ClickHouse数据库
            clickHouseConnection = DriverManager.getConnection(clickHouseUrl, clickHouseUsername, clickHousePassword);




            //创建配置文件语句
            String conffiled = "";
            //创建clickhouse上的字段集合，用来选择orderby后面的字段
            List<String> columnNames = new ArrayList<>();

            // 创建ClickHouse建表语句
            String clickHouseCreateTableQuery = "CREATE TABLE " + cktable + "(";
            while (sqlServerResultSet.next()) {
                String columnName = sqlServerResultSet.getString("COLUMN_NAME");
                String dataType = sqlServerResultSet.getString("DATA_TYPE");
                String columnName1 = chinesetopinyin.chineseToPinyin(columnName);
                String dataType1 = getClickhouseColumnType(dataType);
                columnNames.add(columnName1);


                //输出展示一下字段值和字段类型
                System.out.println(columnName1 + ' ' + dataType1);

                // 加个东西给string类型的去除一下空格符，防止串行的出现
                if(dataType1 == "String"){
                    //columnName = "REPLACE(REPLACE(`" + columnName + "`, '\\n', ''), '\\t', '')";
                    columnName = "REPLACE(REPLACE(cast(`" + columnName + "` as string), '\\n', ''), '\\t', '')";
                }else  columnName = "`" + columnName + "`";
                conffiled +=  columnName +  " " +  columnName1  +  ",";
                clickHouseCreateTableQuery += columnName1 + " " + dataType1 + ",";
            }

            //选择第一个作为order by后面的字段
            String orderbyfield = columnNames.get(0);

            // 去除最后一个逗号
            clickHouseCreateTableQuery = clickHouseCreateTableQuery.substring(0, clickHouseCreateTableQuery.length() - 1);
            clickHouseCreateTableQuery += ") ENGINE = MergeTree() ORDER BY "+ orderbyfield;

            //去除最后一个逗号
            conffiled = conffiled.substring(0, conffiled.length() - 1);

            //
           conffiled = "sql{ sql=" + "\"select " + conffiled +   " from result\"}"   ;


            System.out.println(clickHouseCreateTableQuery);
            System.out.println(conffiled);

            // 检查表是否存在
            // 连接到ClickHouse数据库

            //检查clickhouse中该表是否存在   
            DatabaseMetaData clickHouseMetaData = clickHouseConnection.getMetaData();
            ResultSet tablesResultSet = clickHouseMetaData.getTables(null, null, cktable, null);

            // 如果表不存在则创建表
            if (!tablesResultSet.next()) {
                 Statement clickHouseStatement = clickHouseConnection.createStatement();
                 try {
                     clickHouseStatement.execute(clickHouseCreateTableQuery);
                     System.out.println("ClickHouse建表成功!");
                 }catch (Exception  e ){
                     System.out.println("该表已经存在");
                 }

            }




        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            // 关闭所有连接和资源
            try {
                if (sqlServerResultSet != null) {
                    sqlServerResultSet.close();
                }
                if (sqlServerStatement != null) {
                    sqlServerStatement.close();
                }
                if (sqlServerConnection != null) {
                    sqlServerConnection.close();
                }

                if (clickHouseConnection != null) {
                    clickHouseConnection.close();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //sqlserver对clickhouse的数据类型转换
    public static String getClickhouseColumnType(String dataType) {
        String columnType;
        switch (dataType) {
            case "varchar":
            case "nvarchar":
            case "char":
                columnType = "String";
                break;
            case "int":
                columnType = "Int32";
                break;
            case "bit":
                columnType = "UInt8";
                break;
            case "decimal":
            case "float":
                columnType = "Float64";
                break;
            case "datetime":
                columnType = "DateTime";
                break;
            case "bigint":
                columnType = "Int64";
                break;
            default:
                columnType = "Unknown";
                break;
        }
        return columnType;
    }

}
