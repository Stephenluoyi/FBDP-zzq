package students;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class HBase {

    private static Connection connection;
    static {
        Configuration configuration = HBaseConfiguration.create();
        // configuration.set("hbase.zookeeper.property.clientPort", "2181");
        // 如果是集群 则主机名用逗号分隔
        configuration.set("hbase.zookeeper.quorum", "zero");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * admin 创建 HBase 表
     *
     * @param tableName      表名
     * @param columnFamilies 列族的数组
     */
    // public static boolean createTable(String tableName , List<String>
    // columnFamilies) {
    public static boolean createTable(String tableName) {
        System.out.println("start create table ......");
        try {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                return false;
            }
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

            ColumnFamilyDescriptorBuilder cfDescriptorBuilder = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes("ID"));
            ColumnFamilyDescriptor familyDescriptor = cfDescriptorBuilder.build();
            tableDescriptor.setColumnFamily(familyDescriptor);
            cfDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("Description"));
            familyDescriptor = cfDescriptorBuilder.build();
            tableDescriptor.setColumnFamily(familyDescriptor);
            cfDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("Courses"));
            familyDescriptor = cfDescriptorBuilder.build();
            tableDescriptor.setColumnFamily(familyDescriptor);
            cfDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("Home"));
            familyDescriptor = cfDescriptorBuilder.build();
            tableDescriptor.setColumnFamily(familyDescriptor);

            admin.createTable(tableDescriptor.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * admin 删除 hBase 表
     * 
     * @param tableName 表名
     */
    public static boolean deleteTable(String tableName) {
        try {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            // 删除表前需要先禁用表
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * admin 增加新的列族
     * 
     * @param tableName 表名,familyName 列族名
     */
    public static boolean addAFamily(String tableName, String familyName) throws IOException {
        try {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            TableName tablename = TableName.valueOf(tableName);
            admin.disableTable(tablename);
            ColumnFamilyDescriptorBuilder cfDescriptorBuilder = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(familyName));
            ColumnFamilyDescriptor familyDescriptor = cfDescriptorBuilder.build();
            admin.addColumnFamily(TableName.valueOf(tableName), familyDescriptor);
            /*
             * HTableDescriptor hDescriptor = admin.getTableDescriptor(tablename);
             * HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(familyName);
             * hDescriptor.modifyFamily(hColumnDescriptor); admin.modifyTable(tablename,
             * hDescriptor);
             */
            admin.enableTable(tablename);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 插入数据
     *
     * @param tableName        表名
     * @param rowKey           唯一标识
     * @param columnFamilyName 列族名
     * @param qualifier        列标识
     * @param value            数据
     */
    public static boolean putRow(String tableName, String rowKey, String columnFamilyName, String qualifier,
            String value) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 插入数据
     *
     * @param tableName        表名
     * @param rowKey           唯一标识
     * @param columnFamilyName 列族名
     * @param pairList         列标识和值的集合
     */
    /*
     * public static boolean putRow(String tableName, String rowKey, String
     * columnFamilyName, List<Pair<String, String>> pairList) { try { Table table =
     * connection.getTable(TableName.valueOf(tableName)); Put put = new
     * Put(Bytes.toBytes(rowKey)); pairList.forEach(pair ->
     * put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(pair.getKey()),
     * Bytes.toBytes(pair.getValue()))); table.put(put); table.close(); } catch
     * (IOException e) { e.printStackTrace(); } return true; }
     */

    /**
     * 根据 rowKey 获取指定行的数据
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     */
    public static Result getRow(String tableName, String rowKey) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取指定行指定列 (cell) 的最新版本的数据
     *
     * @param tableName    表名
     * @param rowKey       唯一标识
     * @param columnFamily 列族
     * @param qualifier    列标识
     */
    public static String getCell(String tableName, String rowKey, String columnFamily, String qualifier) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            if (!get.isCheckExistenceOnly()) {
                get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                Result result = table.get(get);
                byte[] resultValue = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                return Bytes.toString(resultValue);
            } else {
                return null;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 检索全表
     *
     * @param tableName 表名
     */
    public static ResultScanner getScanner(String tableName) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 检索表中指定数据
     *
     * @param tableName  表名
     * @param filterList 过滤器
     */

    public static ResultScanner getScanner(String tableName, FilterList filterList) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setFilter(filterList);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 检索表中指定数据
     *
     * @param tableName   表名
     * @param startRowKey 起始 RowKey
     * @param endRowKey   终止 RowKey
     * @param filterList  过滤器
     */

    public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey,
            FilterList filterList) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startRowKey));
            scan.withStopRow(Bytes.toBytes(endRowKey));
            scan.setFilter(filterList);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 浏览表中某一列(族)的数据。 如果该列族有若干列限定符，就列出每个列限定符代表的列的数据；
     * 如果列名以“columnFamily:column”形式给出，只需列出该列的数据。
     * 
     * @param tableName 表名
     * @param column    列名
     */
    public static void scanColumn(String tableName, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        String[] split = column.split(":");
        // 首先判断“列名”的类型，即是“columnFamily”还是“columnFamily:column”
        // 如果是“columnFamily”
        if (split.length == 1) {
            ResultScanner scanner = table.getScanner(column.getBytes());
            for (Result result : scanner) {
                // 获取该列族的所有的列限定符，存放在 cols 中
                Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(column));
                ArrayList<String> cols = new ArrayList<String>();
                for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                    cols.add(Bytes.toString(entry.getKey()));
                }
                // 循环便利，将所有列限定符的值都输出
                for (String str : cols) {
                    System.out
                            .print(str + ":" + new String(result.getValue(column.getBytes(), str.getBytes())) + " | ");
                }
                System.out.println();
            }
            // 释放扫描器
            scanner.close();
        } else {
            // 如果是“columnFamily:column”
            ResultScanner scanner = table.getScanner(split[0].getBytes(), split[1].getBytes());
            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()) + "\t" + column + " -> "
                        + new String(result.getValue(split[0].getBytes(), split[1].getBytes())));
            }
            // 释放扫描器
            scanner.close();
        }
        table.close();
    }

    /**
     * 删除指定行记录
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     */
    public static boolean deleteRow(String tableName, String rowKey) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除指定行指定列
     *
     * @param tableName  表名
     * @param rowKey     唯一标识
     * @param familyName 列族
     * @param qualifier  列标识
     */
    public static boolean deleteColumn(String tableName, String rowKey, String familyName, String qualifier) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier));
            table.delete(delete);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Process starting");
        String tableName = "students";

        // 1.创建students表
        System.out.println("1.创建students表");
        if (createTable(tableName)) {
            putRow(tableName, "001", "Description", "Name", "Li Lei");
            putRow(tableName, "001", "Description", "Height", "176");
            putRow(tableName, "001", "Courses", "Chinese", "80");
            putRow(tableName, "001", "Courses", "Math", "90");
            putRow(tableName, "001", "Courses", "Physics", "95");
            putRow(tableName, "001", "Home", "Province", "Zhejiang");

            putRow(tableName, "002", "Description", "Name", "Han Meimei");
            putRow(tableName, "002", "Description", "Height", "183");
            putRow(tableName, "002", "Courses", "Chinese", "88");
            putRow(tableName, "002", "Courses", "Math", "77");
            putRow(tableName, "002", "Courses", "Physics", "66");
            putRow(tableName, "002", "Home", "Province", "Shanghai");

            putRow(tableName, "003", "Description", "Name", "Xiao Ming");
            putRow(tableName, "003", "Description", "Height", "162");
            putRow(tableName, "003", "Courses", "Chinese", "90");
            putRow(tableName, "003", "Courses", "Math", "90");
            putRow(tableName, "003", "Courses", "Physics", "90");
            putRow(tableName, "003", "Home", "Province", "Beijing");

            System.out.println("createTable and putRow Successfully!");
        }
        // 2.扫描students表
        System.out.println("2.扫描students表");
        ResultScanner Result_Scanner = getScanner(tableName);
        for (Result result : Result_Scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("行键:" + new String(CellUtil.cloneRow(cell)) + "\t" + "列族:"
                        + new String(CellUtil.cloneFamily(cell)) + "\t" + "列名:"
                        + new String(CellUtil.cloneQualifier(cell)) + "\t" + "值:"
                        + new String(CellUtil.cloneValue(cell)) + "\t" + "时间戳:" + cell.getTimestamp());
            }
        }
        // 3.查询学生来自的省
        System.out.println("3.查询学生来自的省");
        try {
            scanColumn(tableName, "Home:Province");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // 4.增加新的列Courses：English
        System.out.println("4.增加新的列Courses：English");
        putRow(tableName, "001", "Courses", "English", "95");
        putRow(tableName, "002", "Courses", "English", "85");
        if (putRow(tableName, "003", "Courses", "English", "98")) {
            System.out.println("putNewRow " + "[Courses:English]" + " Successfully!");
        }
        ;
        // 5.增加新的列族Contact和新列Contact：Email
        System.out.println("5.增加新的列族Contact和新列Contact：Email");
        if (addAFamily(tableName, "Contact")) {
            putRow(tableName, "001", "Contact", "Email", "lilei@qq.com");
            putRow(tableName, "002", "Contact", "Email", "hanmeimei@qq.com");
            putRow(tableName, "003", "Contact", "Email", "xiaoming@qq.com");
        }
        // （额外）扫描students表
        System.out.println("（额外）删除前扫描students表");
        ResultScanner Result_Scanner3 = getScanner(tableName);
        for (Result result : Result_Scanner3) {
            for (Cell cell : result.rawCells()) {
                System.out.println("行键:" + new String(CellUtil.cloneRow(cell)) + "\t" + "列族:"
                        + new String(CellUtil.cloneFamily(cell)) + "\t" + "列名:"
                        + new String(CellUtil.cloneQualifier(cell)) + "\t" + "值:"
                        + new String(CellUtil.cloneValue(cell)) + "\t" + "时间戳:" + cell.getTimestamp());
            }
        }
        // 6.删除students表
        System.out.println("6.删除students表");
        if (deleteTable(tableName)) {
            System.out.println("deleteTable " + tableName + " Successfully!");
        }
    }
}
