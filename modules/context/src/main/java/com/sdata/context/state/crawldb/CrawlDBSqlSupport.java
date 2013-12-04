package com.sdata.context.state.crawldb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.lakeside.core.utils.ApplicationResourceUtils;
import com.lakeside.core.utils.FileUtils;
import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.context.config.Configuration;

/**
 * 提供CrawlDb Sql库的访问支持
 * 
 * @author houdejun
 *
 */
public class CrawlDBSqlSupport {
	
	protected MysqlDataSource dataSource;
	protected NamedParameterJdbcTemplate jdbcTemplate = null;
	protected Configuration conf;
	
	public CrawlDBSqlSupport(Configuration conf){
		this.conf = conf;
		dataSource = CrawlDBDataSource.getDataSource(conf);
		jdbcTemplate = dataSource.getJdbcTemplate();
	}

	/**
	 * init the tables
	 */
	protected void initTables(String tableName,String key) {
		Pattern compile = Pattern.compile("`(\\{[a-zA-Z\\-]*\\})`");
		boolean exists = isTableExists(tableName);
		if(!exists){
			String initScript = getConfigScript(conf,key);
			initScript = compile.matcher(initScript).replaceFirst(tableName);
			execute(initScript);
		}
	}

	private String getConfigScript(Configuration conf, String key) {
		String path = conf.get(key);
		String dbpath = ApplicationResourceUtils.getResourceUrl(CrawlDBSqlSupport.class,path);
		String content;
		try {
			content = FileUtils.readFileToString(dbpath);
		} catch (IOException e) {
			return "";
		}
		return content;
	}

	private boolean isTableExists(String tableName) {
		String sql="SELECT count(1) FROM information_schema.tables WHERE table_schema = :database AND table_name = :tablename LIMIT 1";
		Map<String,Object> parameters = new HashMap<String,Object>();
		String dataBaseName = dataSource.getDatabaseName();
		parameters.put("database", dataBaseName);
		parameters.put("tablename", tableName);
		int result = jdbcTemplate.queryForInt(sql, parameters);
		return result>0;
	}

	private void execute(String sql) {
		Map<String, ?> parameter=null;
		this.jdbcTemplate.update(sql, parameter);
	}

	public Map<String,?>[] trans(List<Map<String,Object>> list) {
		Map<String,?>[] result = new HashMap[list.size()];
		Iterator<Map<String, Object>> iterator = list.iterator();
		int i=0;
		while(iterator.hasNext()){
			result[i] = iterator.next();
			i++;
		}
		return result;
	}
	
	
}
