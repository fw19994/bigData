package com.mysql.test;

import org.junit.Test;

import java.sql.*;

/**
 * @author  fengwei
 */
public class TestMysql {

    @Test
    public void testMysqlStatementTest() throws  Exception {
        String driverClass =  "com.mysql.jdbc.Driver";
        String mysqlUrl = "jdbc:mysql://39.106.51.31:3306/fengwei";
        String userName = "fengwei";
        String password = "317812";

        Class.forName(driverClass);
        Connection conn = DriverManager.getConnection(mysqlUrl,userName,password);
        Statement statement = conn.createStatement();
        String sql = "select * from USER";
        boolean execute = statement.execute(sql);
        ResultSet resultSet = null;
        if(execute){
            resultSet = statement.getResultSet();
            while(resultSet.next()){
                Object con1 = resultSet.getObject(5);
                System.out.println(con1);
            }
        }else {
            System.out.println("数据库查询失败！");
        }

        resultSet.close();
        conn.close();

    }

    @Test
    public void testMysqlJdbcParam () {
        String driverClass = "com.mysql.jdbc.Driver";
        String MysqlUrl = "jdbc:mysql://39.106.51.31:3306/fengwei";
        String userName = "fengwei";
        String password = "317812";
        Connection conn = null;
        String sql = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            Class.forName(driverClass);
            conn= DriverManager.getConnection(MysqlUrl, userName, password);
            sql = "select * from USER ";
            preparedStatement = conn.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                Object o = resultSet.getObject("username");
                System.out.println(o);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(resultSet != null){
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
