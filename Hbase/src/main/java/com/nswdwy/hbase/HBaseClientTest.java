package com.nswdwy.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseClientTest {

    private static Connection connection;

    static {
        try {
            //通过配置类获取配置对象
            Configuration conf = HBaseConfiguration.create();
            //设置zk的地址
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            //通过工厂类获取连接
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    } /**
     * 测试获取连接
     */
    @Test
    public void testConnection(){
        System.out.println(connection);
    }


    //DDL操作：需要用到Admin对象
    /**
     * 测试命名空间的操作
     */
    @Test
    public void testNamespace() throws IOException {
        //获取admin对象
        Admin admin = connection.getAdmin();
        //获取builder对象
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("atguigu");
        //创建命名空间的描述器对象
        NamespaceDescriptor namespaceDescriptor = builder.build();
        //创建命名空间
        admin.createNamespace(namespaceDescriptor);
        //删除命名空间
//        admin.deleteNamespace("atguigu");

        //关闭admin
        admin.close();
    }

    /**
     * 测试表的操作
     */
    @Test
    public void testTable() throws IOException {
        //获取admin对象
        Admin admin = connection.getAdmin();
        //获取TableName对象
        TableName tableName = TableName.valueOf(Bytes.toBytes("atguigu"), Bytes.toBytes("student"));
        //获取builder对象
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);

        //通过Bytes工具类将字符串转换为字节数组
        byte[] info = Bytes.toBytes("info");
        //获取列族的builder对象
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(info);
        //创建列族的描述器对象
        ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
        //设置列族
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);

        //创建表单描述器对象
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        //创建表
        admin.createTable(tableDescriptor);
        //设置表不可用
//        admin.disableTable(tableName);
        //删除表
//        admin.deleteTable(tableName);
        //关闭admin
        admin.close();
    }

    //DML操作：需要用到Table对象
    /**
     * 测试添加或更新数据
     */
    @Test
    public void testPut() throws IOException {
        //获取TableName对象
        TableName tableName = TableName.valueOf(Bytes.toBytes("atguigu"), Bytes.toBytes("student"));
        //获取table对象
        Table table = connection.getTable(tableName);
        //创建put对象
//        Put put = new Put(Bytes.toBytes("1001"));
//        //设置列
//        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("zhangsan"));

        Put put1 = new Put(Bytes.toBytes("1001"));
        //设置列
        put1.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(18));
        Put put2 = new Put(Bytes.toBytes("1001"));
        //设置列
        put2.addColumn(Bytes.toBytes("info"),Bytes.toBytes("email"),Bytes.toBytes("zhangsan@sina.com"));
        //创建一个List
        List<Put> puts = new ArrayList<Put>();
        puts.add(put1);
        puts.add(put2);
        //插入一条数据
//        table.put(put);
        //插入多条数据
        table.put(puts);
        //关闭table
        table.close();
    }

    /**
     * 测试查询一条数据
     */
    @Test
    public void testGet() throws IOException {
        //获取TableName对象
        TableName tableName = TableName.valueOf(Bytes.toBytes("atguigu"), Bytes.toBytes("student"));
        //获取table对象
        Table table = connection.getTable(tableName);
        //创建get对象
        Get get = new Get(Bytes.toBytes("1001"));
//        get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));
        //查询一条数据
        Result result = table.get(get);
        //获取所有的cell
        Cell[] cells = result.rawCells();
        //遍历
        for (Cell cell : cells) {
            //使用CellUtil工具类获取各字段的值
            //获取rowkey
            byte[] rowkey = CellUtil.cloneRow(cell);
            //获取列族
            byte[] family = CellUtil.cloneFamily(cell);
            //获取列名
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            //获取列的值
            byte[] value = CellUtil.cloneValue(cell);
            System.out.println("RowKey是："+Bytes.toString(rowkey));
            System.out.println("列族是："+Bytes.toString(family));
            System.out.println("字段名是："+Bytes.toString(qualifier));
            System.out.println("字段的值是："+Bytes.toString(value));
        }
        //关闭table
        table.close();
    }

    /**
     * 测试扫描数据
     */
    @Test
    public void testScan() throws IOException {
        //获取TableName对象
        TableName tableName = TableName.valueOf(Bytes.toBytes("atguigu"), Bytes.toBytes("student"));
        //获取table对象
        Table table = connection.getTable(tableName);
        //创建scan对象
        Scan scan = new Scan();
        //指定起始的rowkey和结束的rowkey
        scan.withStartRow(Bytes.toBytes("1001"));
        scan.withStopRow(Bytes.toBytes("1005"));
        //获取扫描器
        ResultScanner scanner = table.getScanner(scan);
        //迭代得到result
        for (Result result : scanner) {
            //获取所有的cell
            Cell[] cells = result.rawCells();
            //遍历
            for (Cell cell : cells) {
                //使用CellUtil工具类获取各字段的值
                //获取rowkey
                byte[] rowkey = CellUtil.cloneRow(cell);
                //获取列族
                byte[] family = CellUtil.cloneFamily(cell);
                //获取列名
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                //获取列的值
                byte[] value = CellUtil.cloneValue(cell);
                System.out.println("RowKey是："+Bytes.toString(rowkey));
                System.out.println("列族是："+Bytes.toString(family));
                System.out.println("字段名是："+Bytes.toString(qualifier));
                System.out.println("字段的值是："+Bytes.toString(value));
            }
        }
        //关闭scanner
        scanner.close();
        //关闭table
        table.close();
    }

   @Test
    public void deleteTable() throws IOException {

       Table table = connection.getTable(TableName.valueOf(Bytes.toBytes("atguigu"), Bytes.toBytes("student")));
       Delete delete = new Delete(Bytes.toBytes("1001"));
       table.delete(delete);
       table.close();
   }
}
