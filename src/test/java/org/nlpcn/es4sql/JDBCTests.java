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

        properties.put("url", "jdbc:elasticsearch://192.168.1.157:9300,192.168.1.143:9300,192.168.1.97:9300/index/");
        properties.put("username","123456");
        properties.put("password","123456");
        properties.put("clusterName","test");
        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        Connection connection = dds.getConnection();
        String sql = "SELECT * FROM account where account_id = 2";


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


