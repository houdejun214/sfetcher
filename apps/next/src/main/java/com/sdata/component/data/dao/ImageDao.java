package com.sdata.component.data.dao;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.classic.Session;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.core.data.entity.TagType;

/**
 * @author houdj
 *
 */
public class ImageDao {
	private SessionFactory sessionFactory;
	private final NamedParameterJdbcTemplate jdbcTemplate;
	
	public ImageDao(SessionFactory sessionFactory,NamedParameterJdbcTemplate jdbcTemplate){
		this.jdbcTemplate=jdbcTemplate;
	}
	
	public boolean saveImage(Map<?, ?> image){
		String sql = " INSERT INTO `Image`(`ImageId`, `description`, `filePath`, `fileType`,  "
				+"  `height`, `imageUrl`, `latitude`, `longitude`,                        "
				+"  `originalPhotoId`, `placeName`, `referenceUrl`, `title`,              "
				+"  `width`, `ownerId`, `ownerName`, `siteType`,                          "
				+" `addDate`, `cityName`, `takenDate`, `uploadDate`)                      "
				+" VALUES (:ImageId, :description, :filePath, :fileType,                  "
				+"  :height, :imageUrl, :latitude, :longitude,                            "
				+"  :originalPhotoId, :placeName, :referenceUrl, :title,                  "
				+"  :width, :ownerId, :ownerName, :siteType,                              "
				+" :addDate, :cityName, :takenDate, :uploadDate)                          ";
		Map<String,Object> parameters = new HashMap<String,Object>();
		Long imageId = (Long)image.get("ImageId");
		parameters.put("ImageId", imageId);
		parameters.put("description", getText(image.get("Description"),5000));
		parameters.put("filePath", image.get("FilePath"));
		parameters.put("fileType", image.get("FileType"));
		parameters.put("height", image.get("Height"));
		parameters.put("imageUrl", image.get("ImageUrl"));
		parameters.put("latitude", image.get("Latitude"));
		parameters.put("longitude", image.get("Longitude"));
		parameters.put("originalPhotoId", image.get("OriginalPhotoId"));
		parameters.put("placeName", image.get("CurQuery")); // current query keywords can be seen as the place name of the image.
		parameters.put("referenceUrl", image.get("ReferenceUrl"));
		parameters.put("title", image.get("Title"));
		parameters.put("width", image.get("Width"));
		parameters.put("ownerId", getText(image.get("OwnerId"),100));
		parameters.put("ownerName", getText(image.get("OwnerName"),50));
		parameters.put("siteType", image.get("SiteType"));
		parameters.put("addDate", new Date());
		parameters.put("cityName", image.get("CityName"));
		parameters.put("takenDate", image.get("TakenDate"));
		parameters.put("uploadDate", getDate((String)image.get("UploadDate")));
		jdbcTemplate.update(sql, parameters);
		// save others
		this.saveImageComments(imageId, (List<Map>)image.get("ImageComments"));
		this.saveImageTag(imageId, (List<Map>)image.get("ImageTags"));
		if(image.containsKey("ImageExif")){
			Map exif = (Map)image.get("ImageExif");
			if(!exif.containsKey("imageHeight")) exif.put("imageHeight", image.get("Height"));
			if(!exif.containsKey("imageWidth")) exif.put("imageWidth", image.get("Width"));
			this.saveImageExif(imageId, exif);
		}
		return true;
	}
	
	private Date getDate(String str){
		if(StringUtils.isNum(str)){
			return DateTimeUtils.getTimeFromUnixTime(str);
		}else{
			return new Date(str);
		}
	}
	
	private boolean saveImageTag(Long imageId,List<?> tags){
		if(tags==null){
			return false;
		}
		for(Object tag : tags){
			String sql ="INSERT INTO `ImageTag`(`imageId`, `tag`, `tagType`) VALUES (:imageId, :tag, :tagType)";
			Map<String,Object> parameters = new HashMap<String,Object>();
			parameters.put("imageId", imageId);
			parameters.put("tag", getText(tag,500));
			parameters.put("tagType", String.valueOf(TagType.TextTag.ordinal()));
			jdbcTemplate.update(sql, parameters);
		}
		return true;
	}
	
	private boolean saveImageComments(Long imageId,List<Map> comments){
		if(comments==null){
			return false;
		}
		for(Map comment : comments){
			String sql =" INSERT INTO `ImageComment`(`comment`, `commentTime`, "
					+" `imageId`, `ownerId`, `ownerName`)                                     "
					+" VALUES (:comment, :commentTime,                       "
					+" :imageId, :ownerId, :ownerName)                                        ";
			Map<String,Object> parameters = new HashMap<String,Object>();
			parameters.put("comment", getText(comment.get("Comment"),5000));
			parameters.put("commentTime", getDate((String)comment.get("CommentTime")));
			parameters.put("imageId", imageId);
			parameters.put("ownerId", getText(comment.get("OwnerId"),100));
			parameters.put("ownerName", getText(comment.get("OwnerName"),50));
			jdbcTemplate.update(sql, parameters);
		}
		return true;
	}
	
	private boolean saveImageExif(Long imageId,Map exif){
		if(exif==null){
			return false;
		}
		String sql =" INSERT INTO `ImageExif`(`imageId`, `apertureValue`, `dateTime`, "
				+" `exposureBias`, `exposureTime`, `fNumber`,                      "
				+"  `flash`, `focalLength`, `imageHeight`,                         "
				+"  `imageWidth`, `isoSpeed`, `camera`)                            "
				+"  VALUES (:imageId, :apertureValue, :dateTime,                   "
				+" :exposureBias, :exposureTime, :fNumber,                         "
				+"  :flash, :focalLength, :imageHeight,                            "
				+"  :imageWidth, :isoSpeed, :camera)                               ";
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("imageId", imageId);
		parameters.put("apertureValue", exif.get("apertureValue"));
		parameters.put("dateTime", exif.get("dateTime"));
		parameters.put("exposureBias", exif.get("exposureBias"));
		parameters.put("exposureTime", exif.get("exposureTime"));
		parameters.put("fNumber", exif.get("fNumber"));
		parameters.put("flash", exif.get("flash"));
		parameters.put("focalLength", exif.get("focalLength"));
		parameters.put("imageHeight", exif.get("imageHeight"));
		parameters.put("imageWidth", exif.get("imageWidth"));
		parameters.put("isoSpeed", exif.get("isoSpeed"));
		parameters.put("camera", exif.get("camera"));
		jdbcTemplate.update(sql, parameters);
		return true;
	}
	
	public int getCountByOriginalId(String originalId){
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.put("originalPhotoId", originalId);
		String sql = "select count(1) from Image where originalPhotoId=:originalPhotoId ";
		return this.jdbcTemplate.queryForInt(sql, paramMap);
	}
	
	public void deleteImage(Long imageId) {
		if(imageId==null){
			return;
		}
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.put("imageId", imageId);
		this.jdbcTemplate.update("delete from ImageTag where imageId=:imageId", paramMap);
		this.jdbcTemplate.update("delete from ImageComment where imageId=:imageId", paramMap);
		this.jdbcTemplate.update("delete from ImageExif where imageId=:imageId", paramMap);
		this.jdbcTemplate.update("delete from Image where imageId=:imageId", paramMap);
	}
	
	public Transaction beginTransaction(){
		Transaction transaction = sessionFactory.getCurrentSession().beginTransaction();
		transaction.begin();
		return transaction;
	}
	
	public void commitTransaction(){
		Session currentSession = sessionFactory.getCurrentSession();
		Transaction transaction = currentSession.getTransaction();
		transaction.commit();
	}
	
	public void clearSession(){
		Session currentSession = sessionFactory.getCurrentSession();
		currentSession.clear();
	}
	
	public void rollbackTransaction(){
		Transaction transaction = sessionFactory.getCurrentSession().getTransaction();
		transaction.rollback();
	}
	
	public String getText(Object origin,int maxLength){
		String str = "";
		if(origin!=null){
			str = origin.toString();
		}
		int valueLen = StringUtils.byteLenth(str);
		if( valueLen > maxLength){
			return StringUtils.splitString(str, maxLength);
		}else{
			return str;
		}
	}
}
