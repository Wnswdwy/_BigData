package com.nswdwy.thick;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;


/**
 * @author yycstart
 * @create 2020-10-13 14:44
 */
public class ThickClientTest {
    private Connection connection = null;
    @Before
    public void init(){
        String url = "jdbc:phoenix:Hadoop102,Hadoop103,Hadoop104:2181";
        Properties properties = new Properties();
        properties.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
        try {
            connection = DriverManager.getConnection(url, properties);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }
    @After
    public void close(){
        if(connection != null) {
            try {
                connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    @Test
    public void createTableTest() throws SQLException {
        String sql = "creta table \"studented\"(id varchar primary key,info.name varchar,info.age unsigned_long)";

        PreparedStatement prep = connection.prepareStatement(sql);
        prep.execute();
        connection.commit();
        prep.close();
    }

    /**
     *
     */
   @Test
   public void getTest() throws SQLException {
       String sql = "select * from \"studented\"";
       PreparedStatement prep = connection.prepareStatement(sql);
       ResultSet resultSet = prep.executeQuery();
       while(resultSet.next()){
           String id = resultSet.getString(1);
           String name = resultSet.getString(2);
           Object age = resultSet.getObject(3);
           System.out.println("id = "+ id );
           System.out.println("name = "+ name );
           System.out.println("age = "+ age );
       }
   }

   @Test
   public void addTest() throws SQLException {
       String sql = "upsert into \"studented\" values(?,?,?)";

       PreparedStatement prep = connection.prepareStatement(sql);
       prep.setString(1,"1001");
       prep.setString(2,"zhangsan");
       prep.setObject(3,13L);
       prep.executeUpdate();
       //手动提交
       connection.commit();
       prep.close();
   }

    @Test
    public void getConnection(){
        System.out.println(connection);
    }

    @Test
    public void testConnection(){
        String url = "jdbc:phoenix:Hadoop102,Hadoop103,Hadoop104:2181";
        Properties properties = new Properties();
        properties.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");

        Connection   connection = null;
        try {
            connection = DriverManager.getConnection(url, properties);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        System.out.println(connection);


    }
}
