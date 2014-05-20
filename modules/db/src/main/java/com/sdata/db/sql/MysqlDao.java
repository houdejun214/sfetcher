package com.sdata.db.sql;

import com.lakeside.data.sqldb.MysqlDataSource;
import com.lakeside.data.sqldb.XqlBuilder;
import com.sdata.db.BaseDao;
import com.sdata.db.Collection;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by dejun on 19/05/14.
 */
public class MysqlDao extends BaseDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private MysqlDataSource dataSource;

    public MysqlDao(MysqlDataSource dataSource, Collection collection) {
        super(collection);
        this.dataSource = dataSource;
        this.jdbcTemplate = dataSource.getJdbcTemplate();
    }

    @Override
    public boolean save(Map<String, Object> data) {
        String collectionName = this.collection.getName();
        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            String key = next.getKey();
            Object value = next.getValue();
            XqlBuilder builder = new XqlBuilder();
        }
        return false;
    }

    @Override
    public boolean isExists(Object id) {
        return false;
    }

    @Override
    public void delete(Object id) {
        
    }
}
