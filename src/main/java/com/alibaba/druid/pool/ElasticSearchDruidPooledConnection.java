package com.alibaba.druid.pool;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by allwefantasy on 8/30/16.
 */
public class ElasticSearchDruidPooledConnection extends DruidPooledConnection {
    public ElasticSearchDruidPooledConnection(DruidConnectionHolder holder) {
        super(holder);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkState();

        PreparedStatementHolder stmtHolder = null;
        ElasticSearchConnection elasticSearchConnection = (ElasticSearchConnection)conn;
        if(elasticSearchConnection.getIndex() != null && elasticSearchConnection.getIndex().length()>0){
            MySqlStatementParser parser = new MySqlStatementParser(sql);
            SQLStatement statement = parser.parseStatement();
            MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
            statement.accept(visitor);
            String tableName = visitor.getCurrentTable();
            sql = sql.replaceAll(tableName,elasticSearchConnection.getIndex()+"/"+tableName);
        }


        DruidPooledPreparedStatement.PreparedStatementKey key = new DruidPooledPreparedStatement.PreparedStatementKey(sql, getCatalog(), PreparedStatementPool.MethodType.M1);

        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        if (poolPreparedStatements) {
            stmtHolder = holder.getStatementPool().get(key);
        }

        if (stmtHolder == null) {
            try {
                stmtHolder = new PreparedStatementHolder(key, conn.prepareStatement(sql));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex);
            }
        }

        initStatement(stmtHolder);

        DruidPooledPreparedStatement rtnVal = new ElasticSearchDruidPooledPreparedStatement(this, stmtHolder);

        holder.addTrace(rtnVal);

        return rtnVal;
    }

    private void initStatement(PreparedStatementHolder stmtHolder) throws SQLException {
        stmtHolder.incrementInUseCount();
        holder.getDataSource().initStatement(this, stmtHolder.getStatement());
    }

}
