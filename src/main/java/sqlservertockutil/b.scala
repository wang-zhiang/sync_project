

import java.sql.DriverManager
import java.sql.Connection

object b {
  def main(args: Array[String]): Unit = {
    // JDBC连接信息
    val driver = "org.apache.hive.jdbc.HiveDriver"
    val url = "jdbc:hive2://192.168.5.104:10000"
    val username = "smartpath"
    val password = "cl@1109"

    // 加载Hive JDBC驱动
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    // 建立连接
    val connection = DriverManager.getConnection(url, username, password)

    // 执行查询
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("SELECT * FROM tmp.sp_1")

    // 解析查询结果
    while (resultSet.next()) {
      val field1 = resultSet.getString("pt_channel")
      val field2 = resultSet.getString("pt_ym")
      // 处理结果
      print(field1 +  " " + field2)
    }

    // 关闭连接
    resultSet.close()
    statement.close()
    connection.close()
  }
}
