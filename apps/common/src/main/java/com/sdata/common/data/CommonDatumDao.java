package com.sdata.common.data;

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
public class CommonDatumDao extends BaseDao{
	
	private static Logger log = LoggerFactory.getLogger("SdataCrawler.FashionBlogImageDao");

	public CommonDatumDao(MysqlDataSource datasource) {
		this.jdbcTemplate = datasource.getJdbcTemplate();
	}

	public void save(Map<String,Object> data){
        String purl =StringUtils.valueOf(data.get("url"));
		String productId =StringUtils.valueOf(data.get("productId"));
		if(productId == null ){
			throw new RuntimeException("the property of Id is empty!");
		}
        Object img = data.get("image");
        if(img instanceof List){
            List<String> imagesList = (List<String>) img;
            if(imagesList==null || imagesList.size()==0){
                log.warn("have no images in["+purl+"]");
                return;
            }
            data.put("image",imagesList.get(0));
        }else if(StringUtils.isEmpty(String.valueOf(img))){
            log.warn("have no images in["+purl+"]");
            return;
        }
		this.saveProduct(data);
	}

    /**
     * check if the product exists
     * @param datum
     * @return
     */
    public boolean checkExists(Map<String, Object> datum){
        String productId =StringUtils.valueOf(datum.get("productId"));
        if(StringUtils.isEmpty(productId)){
            return false;
        }
        String sql = "select * from `production` where `url` = :url and `domain`=:domain and `category`=:category";
        List<Map<String, Object>> results = this.jdbcTemplate.queryForList(sql, datum);
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
			param.put("productId", data.get("productId"));
			param.put("category", data.get("category"));
			param.put("url", data.get("url"));
            String topImage = StringUtils.valueOf(data.get("image"));
            param.put("image_url", topImage);
            param.put("domain", data.get("domain"));
			param.put("description", data.get("description"));
            param.put("fetch_time", data.get(Constants.FETCH_TIME));
            String sql = "INSERT INTO `production`(`productId`," +
                    "`category`," +
                    "`url`,\n" +
                    "`domain`,\n" +
                    "`image_url`,\n" +
                    "`description`,\n" +
                    "`fetch_time`) "
                    + "VALUES(:productId,:category,:url,:domain, :image_url, :description,:fetch_time)";
            this.jdbcTemplate.update(sql, param);
		} catch (Exception e) {
            e.printStackTrace();
            log.info("duplicate datum [{}]",data.get("productId"));
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
