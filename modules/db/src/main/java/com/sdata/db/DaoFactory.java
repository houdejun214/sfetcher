package com.sdata.db;

import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.context.config.Configuration;
import com.sdata.db.sql.MysqlDao;

import java.util.HashMap;
import java.util.Map;

/**
 *
 *
 */
public class DaoFactory {

    private static Map<Configuration,MysqlDataSource> dataSources = new HashMap<Configuration,MysqlDataSource>();
	private static Map<Collection,BaseDao> daos = new HashMap<Collection,BaseDao>();


    public static BaseDao getDao(Configuration conf, Collection collection) {
        if(!daos.containsKey(collection)){
            synchronized (conf) {
                if(!daos.containsKey(collection)) {
                    MysqlDataSource dataSource = getDataSource(conf);
                    BaseDao dao = new MysqlDao(dataSource,collection);
                    daos.put(collection, dao);
                }
            }
        }
        return daos.get(conf);
    }

    public static MysqlDataSource getDataSource(Configuration conf){
        if(!dataSources.containsKey(conf)){
            synchronized (conf) {
                if(!dataSources.containsKey(conf)){
                    String host = conf.get("storer.mysql.jdbc.host");
                    String database = conf.get("storer.mysql.jdbc.database");
                    String username = conf.get("storer.mysql.jdbc.username");
                    String password = conf.get("storer.mysql.jdbc.password");
                    Integer port = conf.getInt("storer.mysql.jdbc.port", 3306);
                    MysqlDataSource mysqlDataSource = new MysqlDataSource(host, port.toString(), database, username, password);
                    dataSources.put(conf,mysqlDataSource);
                }
            }
        }
        return dataSources.get(conf);
    }
}
