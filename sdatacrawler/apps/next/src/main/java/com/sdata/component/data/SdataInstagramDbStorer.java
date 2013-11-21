package com.sdata.component.data;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.FileUtils;
import com.lakeside.core.utils.PathUtils;
import com.lakeside.core.utils.SimpleImageInfo;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.component.data.dao.ImageMgDao;
import com.sdata.component.data.dao.UserMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RunState;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.SdataStorer;

/**
 * save user relationship to database
 * 
 * @author geyong
 *
 */
public class SdataInstagramDbStorer extends SdataStorer {
	
	private static final Logger log = LoggerFactory.getLogger("SdataFlickrUserRelationDbStorer");
	
	private UserMgDao userdao = new UserMgDao();
	private ImageMgDao imagedao = null;
	
	private FieldProcess fieldProcess;
	
	public SdataInstagramDbStorer(Configuration conf,RunState state){
		this.setConf(conf);
		this.state = state;
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		userdao.initilize(host,port,dbName);
		imagedao= new ImageMgDao(conf.get(Constants.SOURCE));
		fieldProcess = new FieldProcess(conf);
	}

	@Override
	public void save(FetchDatum datum) throws Exception {
		Map<String, Object> map = datum.getMetadata();
		ArrayList<HashMap<String, Object>> users = (ArrayList<HashMap<String, Object>>) map.get(Constants.PARSER_TYPE_USERLIST);
		for(int m=0;m<users.size();m++){
			HashMap<String, Object> user = users.get(m);
			if(null!=user){
				this.saveUser(user);
			}
		}
		map.remove(Constants.PARSER_TYPE_USERLIST);
		this.saveImage(map);
	}

	public void saveUser(Map<String, Object> user) throws Exception{
		user = fieldProcess.fieldReduce(user);
		Object id = user.get(Constants.OBJECT_ID);
		if(id ==null ){
			id = StringUtils.getMd5UUID(String.valueOf(user.get(Constants.USER_ID)));
			user.put(Constants.OBJECT_ID, id);
		}else if(!(id instanceof Long || id instanceof UUID)){
			String uId = StringUtils.valueOf(id);
			if("".equals(uId))
				throw new RuntimeException("the property of userId is empty!");
			id = Long.valueOf(uId);
			user.put(Constants.OBJECT_ID, id);
		}
		userdao.saveUser(user);
	}
	
	public void saveImage(Map<String, Object> metadata) throws Exception {
		String  absoluteFilePath = "";
		UUID imageId = null;
		try {
			String imageUrl = ((Map<String, Object>)((Map<String, Object>)metadata.get(Constants.IMAGES)).get("standard_resolution")).get("url").toString();
			String imgId = (String)metadata.get(Constants.USER_ID) ;
			try {
				imageId = StringUtils.getMd5UUID(imgId);
			} catch (Exception e) {
				throw new NegligibleException("originalPhotoId is empty or abnormal!");
			}
			//remove url if url is null
			Object url = metadata.get("url");
			if(null==url){
				metadata.remove("url");
			}
			String filePath = getFilePath(getOriginalPhotoId(imageUrl));
			Map<String, Object> imgurl= new HashMap<String, Object>();
			imgurl.put("local",filePath);
			imgurl.put("imageurl", imageUrl);
			metadata.put("filePath",imgurl);
			metadata.put(Constants.FETCH_TIME, new Date());
			String created_time=(String)metadata.get("created_time");
			metadata.remove("created_time");
			metadata.put("crtdt", getDate(created_time));
			absoluteFilePath = saveImageFile(imageUrl, filePath);
			this.getImageSize(metadata, absoluteFilePath);
			metadata.put("orgid", imageId);
			metadata.put("_id", imageId);
			metadata = fieldProcess.fieldReduce(metadata);
			imagedao.saveImage(metadata,fieldProcess);
		} catch (Exception e) {
			logFaileMessage(e);
			throw e;
		}
	}
	
	private String getOriginalPhotoId(String url){
		if(url.endsWith("/")){
			url = url.substring(0,url.length()-1);
		}
		int start = url.lastIndexOf("/");
		String photoId = url.substring(start+1);
		return photoId;
	}
	
	private Date getDate(String str){
		if(StringUtils.isNum(str)){
			return DateTimeUtils.getTimeFromUnixTime(str);
		}else{
			return new Date(str);
		}
	}

	private String saveImageFile(String imageUrl, String filePath) throws IOException, MalformedURLException {
		String dataDirectory = getConf("DataDir");
		if(StringUtils.isEmpty(dataDirectory)){
			throw new RuntimeException("data directory have not be specified! ");
		}
		String absoluteFilePath= PathUtils.getPath(dataDirectory+"/"+filePath);
		
		// insure the file directory is existent
		FileUtils.insureFileDirectory(absoluteFilePath);
		if(FileUtils.fileExists(absoluteFilePath)){
			return absoluteFilePath;
		}
		// save as image file
		try{
			org.apache.commons.io.FileUtils.copyURLToFile(new URL(imageUrl), new File(absoluteFilePath));
		}catch(IOException e){
			String message = e.getMessage();
			if(message.startsWith("Server returned HTTP response code: 504 for URL")){
				//try download the image again;
				org.apache.commons.io.FileUtils.copyURLToFile(new URL(imageUrl), new File(absoluteFilePath));
			}
		}
		return absoluteFilePath;
	}
	
	public String getFilePath(String imageId){
		//String siteType = image.getSiteType();
		String prentStr = getPrent(imageId,3);
		return "/instagram/"+prentStr+"/"+imageId;
	}

	private String getPrent(String imageId,int length) {
		String prentStr = imageId.substring(0, length);
		return prentStr;
	}

	private void getImageSize(Map<String, Object> image,String absoluteFilePath){
		try
        {
			File file = new File(absoluteFilePath);
			SimpleImageInfo imageInfo = new SimpleImageInfo(file);
			image.put("width",imageInfo.getWidth());
			image.put("height",imageInfo.getHeight());
        }
		catch (Exception e1)
        {
        	log.warn("read file size failed by SimpleImageInfo ["+e1.getMessage()+"] :"+absoluteFilePath);
        }
	}
	
	protected void deleteOneImageForException(String filePath,Long imageId){
		try {
			File file=new File(filePath);
			if(file.exists()){
				boolean delete = file.delete();
				if(!delete){
					throw new RuntimeException("delete file ["+filePath+"] failed");
				}
			}
			// delete the saved database records
			imagedao.deleteImage(imageId);
		} catch (Exception e) {
			e.printStackTrace();
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
