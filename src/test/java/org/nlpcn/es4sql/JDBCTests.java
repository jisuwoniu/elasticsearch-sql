package org.nlpcn.es4sql;


import com.alibaba.druid.pool.DruidDataSource;

import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by allwefantasy on 8/26/16.
 */
public class JDBCTests {
    @Test
    public void testJDBC() throws Exception {
        Properties properties = new Properties();

        properties.put("url", "jdbc:elasticsearch://10.95.117.157:9300,10.95.97.143:9300,10.95.96.97:9300/mysql_daijia_kuaipay*/");
        properties.put("username","24304");
        properties.put("password","nal39AE3lgbvw");
        properties.put("clusterName","elk-test");
        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        Connection connection = dds.getConnection();
        String sql = "SELECT * FROM account_flow_statement where account_id = 2";


        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        List<String> result = new ArrayList<String>();
        while (resultSet.next()) {
            result.add(resultSet.getInt("account_id") + "," + resultSet.getInt("rel_account_id") + "," + resultSet.getString("flow_no"));
        }
        System.out.println(result);
        ps.close();
        connection.close();
        dds.close();
        System.out.println(result);
    }

}


