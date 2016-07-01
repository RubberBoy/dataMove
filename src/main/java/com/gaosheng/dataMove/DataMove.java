package com.gaosheng.dataMove;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
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

        tranfsTableData("ACTION_GRID_CONFIG",
                null,
                null);
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
            List<ColumnMeta> columnMetas = DbUtil.getColumnMetaList(fetchDataRs);

            StringBuilder nameSql = new StringBuilder("insert into " + table +"(");
            StringBuilder values = new StringBuilder();
			for (int j =0;j<columnMetas.size();j++){
				nameSql.append(columnMetas.get(j).name+",");
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
				for (int j = 0; j < columnMetas.size();j++){
                    ColumnMeta columnMeta = columnMetas.get(j);
					if(columnMeta.type.equals("DATE")){
						toStatement.setDate(j+1,fetchDataRs.getDate(j+1));
					} else if(columnMeta.type.equals("LONG") || columnMeta.type.equals("CLOB")){
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

}
