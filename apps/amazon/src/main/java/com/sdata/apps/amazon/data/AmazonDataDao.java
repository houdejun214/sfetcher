package com.sdata.apps.amazon.data;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.sqldb.BaseDao;
import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.context.config.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author houdj
 *
 */
public class AmazonDataDao extends BaseDao{
	
	private static Logger log = LoggerFactory.getLogger("SdataCrawler.FashionBlogImageDao");

	public AmazonDataDao(MysqlDataSource datasource) {
		this.jdbcTemplate = datasource.getJdbcTemplate();
	}

	public void save(Map<String,Object> data){
        String purl =StringUtils.valueOf(data.get("purl"));
		String productId =StringUtils.valueOf(data.get("productId"));
		if(productId == null ){
			throw new RuntimeException("the property of Id is empty!");
		}
		List<Map<String,String>> imagesList = (List<Map<String,String>>)data.get("images");
		if(imagesList==null || imagesList.size()==0){
			log.warn("have no images in["+purl+"]");
			return;
		}
		this.saveProduct(data);
	}

    /**
     * check if the product exists
     * @param productId
     * @return
     */
    public boolean checkExists(String productId){
        if(StringUtils.isEmpty(productId)){
            return false;
        }
        Map<String,Object> param = new HashMap<String,Object>();
        param.put("id", productId);
        String sql = "select * from `production` where `id` = :id";
        List<Map<String, Object>> results = this.jdbcTemplate.queryForList(sql, param);
        if(results!=null && results.size()>0) {
            return true;
        }
        return false;
    }


	/**
	 * save page infomation
	 * @param data
	 * @return
	 */
	public void saveProduct(Map<String, Object> data){
		try {
			Map<String,Object> param = new HashMap<String,Object>();
			param.put("id", data.get("productId"));
			param.put("name", data.get("productName"));
			param.put("product_url", data.get("purl"));
            List<String> images = (List<String>) data.get("images");
            String topImage = null;
            if(images!=null && images.size()>0) {
                topImage = images.get(0);
            }
            param.put("image_url", topImage);
			param.put("category", data.get("category"));
			param.put("description", StringUtils.abbreviate(String.valueOf(data.get("description")),990));
            param.put("price", data.get("price"));
            param.put("fetch_time", data.get(Constants.FETCH_TIME));
			String update = "UPDATE `production` SET `name` = :name,`product_url` = :product_url," +
                    "`image_url` = :image_url,`category` = :category," +
                    "`description` = :description," +
                    "`price` =:price " +
                    "WHERE `id` = :id";
			int row = this.jdbcTemplate.update(update, param);
			if(row <=0){
				String sql = "INSERT INTO `production`(`id`," +
                        "`name`," +
                        "`product_url`,\n" +
                        "`image_url`,\n" +
                        "`category`,\n" +
                        "`description`,\n" +
                        "`price`," +
                        "`fetch_time`) "
						+ "VALUES(:id,:name,:product_url,:image_url,:category,:description,:price,:fetch_time)";
                this.jdbcTemplate.update(sql, param);
            } else {
                log.info("duplicate datum [{}]",data.get("productId"));
            }
		} catch (Exception e) {
            e.printStackTrace();
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
