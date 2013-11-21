package com.sdata.component.data.storer;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.lakeside.core.utils.FileUtils;
import com.lakeside.core.utils.StringUtils;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.util.ApplicationResourceUtils;

public class FashionBlogSqlStorer{
	
	private ComboPooledDataSource dataSource = null;
	
	private NamedParameterJdbcTemplate jdbcTemplate = null;
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.CrawlDB");
	
	private static String RunStateTableName="RunResult";
	private Configuration conf;
	private String dataBase;

	private DataSourceTransactionManager transactionManager;

	private DefaultTransactionDefinition def;
	public FashionBlogSqlStorer(Configuration conf){
		this.conf = conf;
		String crawlName = conf.get("CrawlName");
		if(conf.containsKey("CrawlDbTableName")){
			crawlName = conf.get("CrawlDbTableName");
		}
		
		RunStateTableName="RunResult"+WordUtils.capitalize(crawlName);
				
		String userName = conf.get("UserName");
		String password = conf.get("Password");
		dataBase = conf.get("DataBase");
		String host = conf.get("Host");
		String port = conf.get("Port");
		try {
			dataSource = new ComboPooledDataSource();
			dataSource.setDriverClass("com.mysql.jdbc.Driver");
			String url = StringUtils.format("jdbc:mysql://{0}:{1}/{2}?useUnicode=true&characterEncoding=UTF-8&charSet=UTF-8", host,port,dataBase);
			dataSource.setJdbcUrl(url);
			dataSource.setUser(userName);
			dataSource.setPassword(password);
			jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
			def = new DefaultTransactionDefinition();
			transactionManager = new DataSourceTransactionManager(dataSource);
		} catch (PropertyVetoException e) {
			throw new RuntimeException(e);
		}
		this.initTables();
	}
	
	/**
	 * init the tables
	 */
	private void initTables(){
		Pattern compile = Pattern.compile("`(\\{[a-zA-Z\\-]*\\})`");
		boolean exists = isTableExists(RunStateTableName);
		if(!exists){
			String initScript = getConfigScript(conf,"CrawlDBRunStateScriptFile");
			initScript=compile.matcher(initScript).replaceFirst(RunStateTableName);
			execute(initScript);
		}
	}
	
	private String getConfigScript(Configuration conf,String key){
		String path = conf.get(key);
		String dbpath = ApplicationResourceUtils.getResourceUrl(path);
		String content;
		try {
			content = FileUtils.readFileToString(dbpath);
		} catch (IOException e) {
			return "";
		}
		return content;
	}
	
	private boolean isTableExists(String tableName){
		String sql="SELECT count(1) FROM information_schema.tables WHERE table_schema = :database AND table_name = :tablename LIMIT 1";
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("database", dataBase);
		parameters.put("tablename", tableName);
		int result = jdbcTemplate.queryForInt(sql, parameters);
		return result>0;
	}
	
	
	/* (non-Javadoc)
	 * @see com.sdata.component.CrawlDB#updateRunState(java.lang.String, java.lang.String, java.lang.String)
	 */
	public Boolean saveResult(final String key,final String val){
		String sql="update "+RunStateTableName+" set value=:value where siteName=:siteName and `key`=:key";
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("key", key);
		parameters.put(Constants.SOURCE, "fashionBlogs");
		parameters.put("value", val);
		int result = this.jdbcTemplate.update(sql, parameters);
		if(result<=0){
			try {
				sql="insert into "+RunStateTableName+" (`key`,siteName,value) values(:key,:siteName,:value)";
				this.jdbcTemplate.update(sql, parameters);
			} catch (DataAccessException e) {
				log.info("insert runstate exception "+e.getMessage());
			}
		}
		return true;
	}
	
	public Boolean checkKeyHaveSuccess(final String key,final String val){
		String sql="select count(*) from "+RunStateTableName+" where siteName=:siteName and `key`=:key and value>=:value";
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("key", key);
		parameters.put(Constants.SOURCE, "fashionBlogs");
		parameters.put("value", val);
		int result = this.jdbcTemplate.queryForInt(sql, parameters);
		if(result<=0){
			return false;
		}
		return true;
	}
	
	private void execute(String sql){
		Map<String, ?> parameter=null;
		this.jdbcTemplate.update(sql, parameter);
	}
	
	public void close() throws IOException {
		
	}
}
