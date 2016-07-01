package com.gaosheng.dataMove;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class DbUtil {

    private static Logger logger = LogManager.getLogger();

    private Connection connection ;

    /**
     * 是否测试
     *  true : 只输出，不执行实际的数据库更新
     *  false : 执行数据库更新
     */
    public static boolean isTest = false;

    public DbUtil (Connection connection) {
        this.connection = connection;
    }

    /**
     * 执行文件夹中sql文件
     * @return
     */
    public boolean executeDirSql(File file) {
        if (null == file) {
            logger.error("文件参数为空");
            return false;
        }
        if (!file.exists()) {
            logger.error("文件 " + file.getName() + " 不存在");
            return false;
        }
        if (!file.isDirectory()) {
            logger.error(file.getName() + " 不是文件夹");
            return false;
        }

        File[] files = file.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                if (pathname.isFile()) {
                    return true;
                }
                return false;
            }
        });

        //排序 文件名格式「num.xxx.sql」按文件名「num」部分排序
        List<File> list = Arrays.asList(files);
        Collections.sort(list, new Comparator<File>(){
            private int getFileNum(String name) {
                String numStr = name.substring(0,name.indexOf("."));
                try {
                    return Integer.valueOf(numStr);
                } catch (NumberFormatException e) {
                    return -1;
                }
            }

            public int compare(File o1, File o2) {
                if(o1.isDirectory() && o2.isFile())
                    return -1;
                if(o1.isFile() && o2.isDirectory())
                    return 1;
                int o1Num = this.getFileNum(o1.getName());
                int o2Num = this.getFileNum(o2.getName());
                if (o1Num >= 0 && o2Num >= 0) {
                    if (o1Num > o2Num) {
                        return 1;
                    }else {
                        return -1;
                    }
                }else {
                    return o1.getName().compareTo(o2.getName());
                }
            }
        });

        for (File sqlFile : list) {
            if (!executeSqlFile(sqlFile)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 执行sql文件
     * @param file sql文件
     * @return
     */
    public boolean executeSqlFile(File file) {
        logger.info("执行sql文件: " + file.getName());
        BufferedReader bufferedReader = null;
        Statement st = null;

        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file),"utf-8"));

            st = connection.createStatement();

            StringBuffer sb = new StringBuffer();
            String line ;
            while (null != (line = bufferedReader.readLine())) {
                line = line.trim();
                if (line.length() > 0) {
                    if (line.indexOf(";") >= 0) {
                        sb.append(line.replaceAll(";", ""));
                        sb.append(" ");

                        logger.debug(sb.toString());
                        if (!isTest) {
                            st.executeUpdate(sb.toString());
                        }

                        sb = new StringBuffer();
                    } else {
                        sb.append(line);
                        sb.append(" ");
                    }
                }
            }
            if (sb.length() > 0 && sb.toString().trim().length() > 0) {
                logger.debug(sb.toString());
                if (!isTest) {
                    st.executeUpdate(sb.toString());
                }
            }
        } catch (Exception e) {
            logger.error("执行文件 " + file.getName() + " 失败",e);
            return false;
        } finally {
            if (null != bufferedReader) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    logger.error("关闭文件失败",e);
                }
            }
            this.close(st,null);
        }
        return true;
    }

    /**
     * 清空表
     *  级联清空引用此表的表数据
     * @param tableName
     * @param owner
     * @param clearedTables
     * @return
     * @throws SQLException
     */
    public boolean clearTable (String tableName,String owner,Set<String> clearedTables) {
        logger.info("清空表 : " + tableName);

        Statement statement = null;
        try {
            List<TableRelation> tableRelations = this.getTableRelations(tableName,owner);

            if (CollectionUtils.isNotEmpty(tableRelations)) {
                for (TableRelation tableRelation : tableRelations) {
                    String fTableName = tableRelation.fkTable;
                    if (clearedTables.contains(fTableName)) {
                        continue;
                    } else {
                        clearedTables.add(fTableName);

                        int count = getTableSize(fTableName, owner);
                        if (count > 0) {
                            clearTable(fTableName, owner, clearedTables);
                        }
                    }
                }
            }

            logger.debug("delete from " + tableName);
            statement = connection.createStatement();

            if (!isTest) {
                statement.execute("delete from " + owner + "." + tableName);
            }
        } catch (Exception e) {
            logger.error("清空表 " + tableName + " 失败",e);
        } finally {
            this.close(statement,null);
        }

        return true;
    }

    /**
     * 获取引用表 tableName 的关系列表
     * @param tableName
     * @param owner
     * @return
     * @throws SQLException
     */
    public List<TableRelation> getTableRelations(String tableName,String owner) throws SQLException {
        List<TableRelation> list = new ArrayList<TableRelation>();

        StringBuffer sql = new StringBuffer();
        sql.append("select re.constraint_name,                         ");
        sql.append("       re.r_constraint_name,                       ");
        sql.append("       fkey.table_name ftable,                     ");
        sql.append("       fkey.column_name fcolumn,                   ");
        sql.append("       pkey.table_name rtable,                     ");
        sql.append("       pkey.column_name rcolumn                    ");
        sql.append("  from user_constraints re,                        ");
        sql.append("       user_cons_columns fkey,                     ");
        sql.append("       user_cons_columns pkey                      ");
        sql.append(" where re.constraint_name = fkey.constraint_name   ");
        sql.append("   and re.r_constraint_name = pkey.constraint_name ");
        sql.append("   and re.owner = upper('" + owner + "')           ");
        sql.append("   and pkey.table_name = upper('" + tableName + "')");
        sql.append("   and re.constraint_type = 'R'                    ");
        sql.append(" order by re.r_constraint_name                     ");

        logger.debug("获取引用信息sql : {}",sql.toString());

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql.toString());
        try {
            while (rs.next()) {
                TableRelation tableRelation = new TableRelation();
                tableRelation.fkTable = rs.getString("ftable");
                tableRelation.fkColumn = rs.getString("fcolumn");
                tableRelation.akTable = rs.getString("rtable");
                tableRelation.akColumn = rs.getString("rcolumn");
                list.add(tableRelation);
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
        return list;
    }

    /**
     * 获取表数据条数
     * @param tableName
     * @param owner
     * @return
     * @throws SQLException
     */
    public int getTableSize(String tableName,String owner) throws SQLException {
        String countSql = "select count(*) from " + owner + "." + tableName;
        Statement countSt = connection.createStatement();
        ResultSet countRs = countSt.executeQuery(countSql);
        try {
            while (countRs.next()) {
                return countRs.getInt(1);
            }
        } finally {
            if (countRs != null) {
                countRs.close();
            }
        }
        return 0;
    }

    public static List<ColumnMeta> getColumnMetaList(ResultSet rs) throws SQLException{
        List<ColumnMeta> list = new ArrayList<ColumnMeta>();
        ResultSetMetaData rsmd = rs.getMetaData();
        int cols = rsmd.getColumnCount();
        for (int i = 1; i <= cols; i++){
            list.add(new ColumnMeta(
                    rsmd.getColumnName(i),
                    rsmd.getColumnTypeName(i)));
        }
        return list;
    }

    /**
     * 获取sequence下一个值
     * @param sequnceName
     * @return
     */
    public long getSequenceNextVal(String sequnceName) {
        return this.getSequenceVal(sequnceName,"nextval");
    }

    /**
     * 获取sequence当前值
     * @param sequnceName
     * @return
     */
    public long getSequenceCurVal(String sequnceName) {
        return this.getSequenceVal(sequnceName,"currval");
    }

    private long getSequenceVal(String sequnceName,String valType) {
        Statement statement = null;
        ResultSet rs = null;
        try {
            statement = connection.createStatement();
            rs = statement.executeQuery("select " + sequnceName + "." + valType + " from dual");
            while (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            this.close(statement,rs);
        }
        return 0;
    }

    /**
     * 获取创建表sql
     * @param tableName
     * @param newTableName  新表名，可为空
     * @return
     */
    public String getCreateTableSql(String tableName,String newTableName) {
        StringBuffer createSql = new StringBuffer("create table " +
                (StringUtils.isEmpty(newTableName) ? tableName : newTableName) + " (");

        StringBuffer sql = new StringBuffer();
        sql.append("select table_name, column_name, data_type, data_length, column_id ");
        sql.append("  from user_tab_columns                                           ");
        sql.append(" where table_name = '"+ tableName.toUpperCase()+"'                ");
        sql.append(" order by column_id asc                                           ");

        Statement statement = null;
        ResultSet rs = null;
        try {
            statement = connection.createStatement();
            rs = statement.executeQuery(sql.toString());

            while (rs.next()) {
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                int dataLength = rs.getInt("data_length");
                createSql.append(columnName + " " + dataType + "(" + dataLength + "),");
            }
            createSql.deleteCharAt(createSql.length() - 1);
            createSql.append(")");
        } catch (SQLException e) {
            logger.error("获取列定义失败",e);
        } finally {
            this.close(statement,rs);
        }
        return createSql.toString().toLowerCase();
    }

    public void close(Statement statement,ResultSet rs) {
        try {
            if (null != rs) {rs.close();}
            if (null != statement) {statement.close();}
        } catch (SQLException e) {
            logger.error("关闭数据库句柄失败",e);
        }
    }

    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.error("关闭连接失败",e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

}
