package com.sdata.component.data.dao;

import com.lakeside.core.utils.PathUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.sqldb.BaseDao;
import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.core.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * @author houdj
 *
 */
public class FashionBlogImageDao extends BaseDao{
	
	private static Logger log = LoggerFactory.getLogger("FashionBlogImageDao");
	private DefaultTransactionDefinition transactionDefinition;
	private DataSourceTransactionManager transactionManager;
	
	public FashionBlogImageDao(MysqlDataSource datasource) {
		this.jdbcTemplate = datasource.getJdbcTemplate();
		transactionDefinition = datasource.getTransactionDefinition();
		transactionManager = datasource.getTransactionManager();
	}
	

	public void save(Map<String,Object> data){
		String pageUrl =StringUtils.valueOf(data.get("pageUrl"));
		if(pageUrl == null ){
			throw new RuntimeException("the property of Id is empty!");
		}
		List<Map<String,String>> imagesList = (List<Map<String,String>>)data.get("imgs");
		if(imagesList==null || imagesList.size()==0){
			log.warn("have no images in["+pageUrl+"]");
			return;
		}
		long pageId = this.savePage(data);
		Object fetchTime = data.get(Constants.FETCH_TIME);
		for(Map<String,String> img:imagesList){
			String url = img.get("url");
			String alt = img.get("alt");
			String title = img.get("title");
			if(!filterImageFileType(url)){
				this.saveImage(url, pageId,fetchTime,alt,title);
			}
		}
	}
	private static Set<String> excludeTypes = new HashSet<String>(Arrays.asList("gif","png"));
	private boolean filterImageFileType(String fileName){
		String extension = PathUtils.getExtension(fileName);
		if(excludeTypes.contains(extension.trim().toLowerCase())){
			return true;
		}
		return false;
	}

	/**
	 * save page infomation
	 * @param data
	 * @return
	 */
	public long savePage(Map<String, Object> data){
		
		try {
			Map<String,Object> param = new HashMap<String,Object>();
			param.put("page_url", data.get("pageUrl"));
			param.put("page_code", encode(data.get("pageUrl")));
			param.put("title", data.get("title"));
			Object value = data.get("cText");
			param.put("content", MessyFixer.convertContent((String)value));
			param.put("pub_time", data.get("date"));
			param.put("fetch_time", data.get(Constants.FETCH_TIME));
			String update = "UPDATE `vs_page` SET `page_url` = :page_url,`fetch_time` = :fetch_time,`title` = :title,`content` = :content,`pub_time` = :pub_time "
					+ "WHERE `page_code` = :page_code";
			int row = this.jdbcTemplate.update(update, param);
			if(row <=0){
				String sql = "INSERT INTO `annotation`.`vs_page`(`page_url`,`page_code`,`title`,`content`,`pub_time`,`fetch_time`) "
						+ "VALUES(:page_url,:page_code,:title,:content,:pub_time,:fetch_time)";
				long pageId = this.insertWithGeneratedKey(sql, param);
				return pageId;
			}else{
				String sql =" select `id` from `vs_page` where `page_code` = :page_code";
				long pageId = this.jdbcTemplate.queryForLong(sql, param);
				return pageId;
			}
		} catch (Exception e) {
			try {
				MessyFixer.convertContent((String)data.get("cText"));
			} catch (UnsupportedEncodingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			throw new RuntimeException(e);
		}
	}
	
	public int saveImage(String imageUrl,long pageId,Object fetchTime,String alt,String title){
		String code = encode(imageUrl);
		try {
			String update = "UPDATE `vs_image` SET `image_url` = :image_url,`page_id` = :page_id,`fetch_time` = :fetch_time,`alt` = :alt,`title` = :title "
					+ "WHERE `image_code` = :image_code";
			Map<String,Object> param = new HashMap<String,Object>();
			param.put("image_code", code);
			param.put("image_url", imageUrl);
			param.put("page_id", pageId);
			param.put("fetch_time", fetchTime);
			param.put("alt", alt);
			param.put("title", title);
			
			int row = this.jdbcTemplate.update(update, param);
			if(row<=0){
				String sql = "INSERT INTO `vs_image`(`image_code`,`image_url`,`page_id`,`fetch_time`,`alt`,`title`) "
					+ "VALUES(:image_code,:image_url,:page_id,:fetch_time,:alt,:title)";
				return this.jdbcTemplate.update(sql, param);
			}
			return row;
		} catch (DuplicateKeyException de){
			log.debug("duplicate image ["+code+":"+imageUrl+"]");
			return 0;
		} catch (DataAccessException e) {
			throw new RuntimeException(e);
		}
	}
	
	public String encode(Object url){
		String surl = String.valueOf(url).trim();
		return StringUtils.md5Encode(surl);
	}
}
