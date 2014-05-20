package com.sdata.apps.amazon.data;

import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.data.store.SdataStorer;

import java.util.Date;
import java.util.Map;

/**
 * Created by dejun on 19/05/14.
 */
public class AmazonStorer extends SdataStorer {

    private static MysqlDataSource dataSource = null;

    private static Object sync = new Object();

    private AmazonDataDao dao = null;

    public AmazonStorer(Configuration conf,RunState state){
        this.setConf(conf);
        this.state = state;
    }

    public void init(Configuration conf){
        if(dataSource==null){
            synchronized (sync) {
                if(dataSource==null){
                    String host = conf.get("storer.mysql.jdbc.host");
                    String database = conf.get("storer.mysql.jdbc.database");
                    String username = conf.get("storer.mysql.jdbc.username");
                    String password = conf.get("storer.mysql.jdbc.password");
                    Integer port = conf.getInt("storer.mysql.jdbc.port", 3306);
                    dataSource = new MysqlDataSource(host, port.toString(), database, username, password);
                    dao = new AmazonDataDao(dataSource);
                }
            }
        }
    }

    @Override
    public void save(FetchDatum datum) throws Exception {
        init(this.getConf());
        try {
            Map<String, Object> metadata = datum.getMetadata();
            metadata.put(Constants.FETCH_TIME, new Date());
            dao.save(metadata);
        } catch (Exception e) {
            logFaileMessage(e);
            throw e;
        }
    }

    protected void logFaileMessage(Exception e){
        String msg = "save failed : "+e.getMessage();
        Throwable cause = e.getCause();
        if(cause!=null){
            msg+="\r\n cause by :"+cause.getClass()+"\n"+cause.getMessage();
        }
        log.info(msg);
    }
}
