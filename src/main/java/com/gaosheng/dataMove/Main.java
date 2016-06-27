package com.gaosheng.dataMove;

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

public class Main {

    private static String tyxlSduProFile = "tyxlsdu.properties";

    private static String sysProFile = "sys.properties";

    private static Logger logger = LogManager.getLogger();

    public static void main(String[] args) throws SQLException {
        while (true) {
            showMenu();
            System.out.println();
            System.out.println();
        }
    }

    private static void showMenu() {
        System.out.println("======================================================");
//        System.out.println("    1. 旧平台数据预处理                                ");
//        System.out.println("    2. 表结构修改                                     ");
        System.out.println("    1. 迁移数据                                      ");
        System.out.println("    2. 清空数据                                      ");
        System.out.println("    0. 退出                                         ");
        System.out.println("======================================================");
        System.out.print("请选择您要进行的操作(输入对应的数字编号) > ");
        Scanner scanner = new Scanner(System.in);
        try {
            int optNum = scanner.nextInt();
            switch (optNum) {
//                case 1 : preHandleOldData();break;
//                case 2 : dbDDLChange();break;
                case 1 : dataMove();break;
                case 2 : clearTable();break;
                case 0 : System.exit(0);break;
                default: System.out.println("错误的操作编号，请重新选择您的操作:");showMenu();break;
            }
        } catch (InputMismatchException e) {
            System.out.println("操作选择错误，请输入正确的数字编号:");
            showMenu();
        }
    }

    /**
     * 旧平台数据预处理
     */
    private static void preHandleOldData() {
        DbUtil dbUtil = new DbUtil(Connect.getConnection(sysProFile));

        dbUtil.executeDirSql(new File("D:\\山大数据迁移\\预处理sql"));

        try {
            dbUtil.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 数据库表结构修改
     */
    private static void dbDDLChange(){
        DbUtil dbUtil = new DbUtil(Connect.getConnection(sysProFile));

        dbUtil.executeSqlFile(new File("D:\\山大数据迁移\\新平台表结构修改.sql"));

        try {
            dbUtil.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 数据迁移
     * @throws SQLException
     */
    private static void dataMove(){
        DbUtil dbUtil = new DbUtil(Connect.getConnection(sysProFile));

        dbUtil.executeDirSql(new File("D:\\山大数据迁移\\迁移sql"));

        try {
            dbUtil.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 清空数据库
     * @throws SQLException
     */
    private static void clearTable() {
        Connection tyxlSduConn = Connect.getConnection(tyxlSduProFile);
        DbUtil dbUtil = new DbUtil(tyxlSduConn);

        Set<String> clearedTables = new HashSet<String>();
        dbUtil.clearTable("pe_semester","tyxlsdu",clearedTables);

        try {
            dbUtil.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void analysisRef() throws SQLException {
        Connection tyxlSduConn = Connect.getConnection(tyxlSduProFile);
        DbUtil dbUtil = new DbUtil(tyxlSduConn);

        List<TableRelation> list = dbUtil.getTableRelations("entity_course_info","sduwynew");
        if (CollectionUtils.isNotEmpty(list)) {
            for (TableRelation tableRelation : list) {
                logger.info(tableRelation.fkTable + tableRelation.fkColumn);
            }
        }

        dbUtil.close();
    }
}
