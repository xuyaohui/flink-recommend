package flink.utils;

import java.sql.*;

public class MysqlClient {

    private static String URL = "jdbc:mysql://192.168.56.10:3306/con";
    private static String NAME = "admin";
    private static String PASS = "123456";
    private static Statement stmt;

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(URL, NAME, PASS);
            stmt = conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据Id筛选产品
     * @param id
     * @return
     * @throws SQLException
     */
    public static ResultSet selectById(String id) throws SQLException {
        String sql = String.format("select  * from product where product_id = %s",id);
        return stmt.executeQuery(sql);
    }


    public static ResultSet selectUserById(String id) throws SQLException{
        String sql = String.format("select  * from user where user_id = %s",id);
        return stmt.executeQuery(sql);
    }

	public static void main(String[] args) throws SQLException {
		ResultSet resultSet = selectUserById("1");
		while (resultSet.next()) {
			System.out.println(resultSet.getString(2));
		}
    }

}
