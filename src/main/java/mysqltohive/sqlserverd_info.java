package mysqltohive;
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

public class sqlserverd_info {

    public static void main(String[] args) {
        //定义sqlserver表名，ck表名 ,ck库名， sqlserver url ，密码 ，用户等信息 即可完成在clickhouse上面建表

        //clickhouse要指定库名以及表名，不然就是在默认里



        String sqltable ="TradingElmDrugAllCity202309";




        // SQL Server数据库连接信息
       // String sqlServerUrl = "jdbc:sqlserver://smartpath10.tpddns.cn:2988;DatabaseName=TradingDouYin";
        //String sqlServerUrl = "jdbc:sqlserver://192.168.4.201:2422;DatabaseName=Taobao_trading";
        String sqlServerUrl = "jdbc:sqlserver://smartnew.tpddns.cn:24222;DatabaseName=O2O";
        String sqlServerUsername = "CHH";
        String sqlServerPassword = "Y1v606";





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




            //创建配置文件语句
            String conffiled = "";
            //创建clickhouse上的字段集合，用来选择orderby后面的字段
            List<String> columnNames = new ArrayList<>();


            while (sqlServerResultSet.next()) {
                String columnName = sqlServerResultSet.getString("COLUMN_NAME");
                String dataType = sqlServerResultSet.getString("DATA_TYPE");
                String columnName1 = chinesetopinyin.chineseToPinyin(columnName);
                String dataType1 = getClickhouseColumnType(dataType);
                columnNames.add(columnName1);


                //输出展示一下字段值和字段类型
                System.out.println(columnName1 + ' ' + dataType1);

                // 加个东西给string类型的去除一下空格符，防止串行的出现
                if(dataType.contains("char")){
                    //columnName = "REPLACE(REPLACE(`" + columnName + "`, '\\n', ''), '\\t', '')";
                    columnName = "regexp_replace(`" + columnName + "`, '[\\t\\n\\r]', '') ";
                }else  columnName = "`" + columnName + "`";
                //conffiled +=  columnName +  " " +  columnName1  +  ",";
                conffiled +=  columnName +  " " +  ",";

            }



            //去除最后一个逗号
            conffiled = conffiled.substring(0, conffiled.length() - 1);

            //
           conffiled = "sql{ sql=" + "\"select " + conffiled +   " from result\"}"   ;


            System.out.println(conffiled);




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
