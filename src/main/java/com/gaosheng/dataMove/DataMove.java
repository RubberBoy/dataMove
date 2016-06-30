package com.gaosheng.dataMove;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DataMove {

    private static Logger logger = LogManager.getLogger();

    /**
     * 是否测试
     *  true : 只输出，不执行实际的数据库更新
     *  false : 执行数据库更新
     */
    public static boolean isTest = true;

	public static void main(String[] args) {

        tranfsTableData("ACTION_MENU_CONFIG",
                null,
                "FK_ACTION_GRID_CONFIG_ID in ('tch_paperGrade','tch_paperTitle','appointTeaStu','peSelectedTitle','prSelectedTitleTea','graduatePaperStatistic','listApointTeaStu','paperScoreManager','apointTeaStuList','prSelectedTitleAdmin')");
    }
	
	public static void tranfsTableData(String table,String columStr,String condition){
		int flush = 200;
		
		Connection fromConn = null;
		Connection toConn = null;
		Statement fromStatement= null;
        PreparedStatement toStatement = null;
        ResultSet fetchDataRs = null;
		try {
			fromConn = Connect.getConnection("121.properties");
			toConn = Connect.getConnection("122.properties");
			fromStatement= fromConn.createStatement();
			
			Date startDate = new Date();
			double totalPay = 0;

			int recordNum = 0;
			
			String searchSql;
			if (StringUtils.isNotEmpty(columStr)) {
				searchSql = "select " + columStr + " from " +table;
			}else{
				searchSql = "select * from "+table ; 
			}
			 
			if (StringUtils.isNotEmpty(condition)) {
				searchSql += " where " + condition;
			}

            fetchDataRs = fromStatement.executeQuery(searchSql);
			StringBuilder nameSql = new StringBuilder("insert into " + table +"(");
			StringBuilder values = new StringBuilder();
			List<String> list = getMetaList(fetchDataRs);
			List<String> typeList = getMetaTypeList(fetchDataRs);
			for (int j =0;j<list.size();j++){
				nameSql.append(list.get(j)+",");
				values.append("?,");
			}
			nameSql.deleteCharAt(nameSql.length()-1);
			nameSql.append(") values( ");
			values.deleteCharAt(values.length()-1);
			nameSql.append(values.toString());
			nameSql.append(")");

            logger.debug(nameSql.toString());
			
			toStatement = toConn.prepareStatement(nameSql.toString());
			
			while(fetchDataRs.next()){
				for (int j = 0; j < list.size();j++){
					if(typeList.get(j).equals("DATE")){
						toStatement.setDate(j+1,fetchDataRs.getDate(j+1));
					} else if(typeList.get(j).equals("LONG") || typeList.get(j).equals("CLOB")){
						String temp = fetchDataRs.getString(j+1);
						if ( temp != null) {
							Reader reader = new StringReader(temp);
							toStatement.setCharacterStream(j+1,reader);
						} else {
							toStatement.setString(j+1,null);
						}
					} else {
                        toStatement.setString(j+1, fetchDataRs.getString(j+1));
					}
				}

                if (!isTest) {
                    toStatement.addBatch();
                }

				recordNum++;
				if (recordNum % flush == 0){
                    if (!isTest) {
                        toStatement.executeBatch();
                    }
					logger.info("prompt "+recordNum+" records committed... \n");
					Date nowDate = new Date();
					double temp = (nowDate.getTime() - startDate.getTime()) / 1000;
					logger.info("this:"+(temp - totalPay)+" ,total:"+temp);
					totalPay = temp;
				}
			}
            if (!isTest) {
                toStatement.executeBatch();
            }
			logger.info("prompt "+recordNum+" records loaded... \n");

		}catch(Exception e){
			e.printStackTrace();
		}finally{
			try{
                if (null != fetchDataRs) {fetchDataRs.close();}
                if (null != toStatement) {toStatement.close();}
				if (null != fromStatement) {fromStatement.close();}
				if (null != toConn) {toConn.close();}
				if (null != fromConn) {fromConn.close();}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	public static List<String> getMetaList(ResultSet rs) throws SQLException{
		List<String> list = new ArrayList<String>();
		ResultSetMetaData rsmd = rs.getMetaData();
		int cols = rsmd.getColumnCount();
		for (int i = 1; i <= cols; i++){
			list.add(rsmd.getColumnName(i));
		}
		return list;
	}
	
	public static List<String> getMetaTypeList(ResultSet rs) throws SQLException{
		List<String> list = new ArrayList<String>();
		ResultSetMetaData rsmd = rs.getMetaData();
		int cols = rsmd.getColumnCount();
		for (int i = 1; i <= cols; i++){
			list.add(rsmd.getColumnTypeName(i));
		}
		return list;
	}
}
