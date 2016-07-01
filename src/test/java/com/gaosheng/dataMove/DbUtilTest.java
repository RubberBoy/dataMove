package com.gaosheng.dataMove;

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

public class DbUtilTest {

    private static Logger logger = LogManager.getLogger();

    private DbUtil dbUtil;

    @Before
    public void init() {
        dbUtil = new DbUtil(Connect.getConnection("test.properties"));
    }

    @Test
    public void getTableRelations() {
        logger.info("---- getTableRelations test start ----");

        List<TableRelation> list = null;
        try {
            list = dbUtil.getTableRelations("pe_grade","tyxlsdu");
            logger.info("引用「pe_grade」的表共 {} 个:",list.size());
            for (TableRelation tableRelation : list) {
                logger.info("{}.{}",tableRelation.fkTable,tableRelation.fkColumn);
            }
        } catch (SQLException e) {
            logger.error("查询失败",e);
        }
        Assert.assertTrue("「表引用」测试失败，未取得引用信息", CollectionUtils.isNotEmpty(list));

        logger.info("---- getTableRelations test end ----");
    }

    @Test
    public void getSequenceNextVal() {
        logger.info("---- getSequenceNextVal test start ----");

        long nextVal = dbUtil.getSequenceNextVal("archieve_id");
        long curVal = dbUtil.getSequenceCurVal("archieve_id");
        logger.info("序列 archieve_id 下一值为: " + nextVal);
        Assert.assertTrue("序列取值错误",nextVal == curVal);

        logger.info("---- getSequenceNextVal test end ----");
    }

    @Test
    public void getCreateTableSql(){
        logger.info("---- getCreateTableSql test start ----");

        String createSql = dbUtil.getCreateTableSql("action_column_config",null);
        logger.info(createSql);
        Assert.assertNotNull(createSql);

        createSql = dbUtil.getCreateTableSql("action_column_config","action_column_config_temp");
        logger.info(createSql);
        Assert.assertNotNull(createSql);

        logger.info("---- getCreateTableSql test end ----");
    }

    @After
    public void destroy(){
        dbUtil.close();
    }
}
