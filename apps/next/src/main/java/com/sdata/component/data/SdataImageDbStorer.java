package com.sdata.component.data;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.FileUtils;
import com.lakeside.core.utils.PathUtils;
import com.lakeside.core.utils.SimpleImageInfo;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.component.data.dao.ImageMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RunState;
import com.sdata.core.data.DataWebSiteType;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.SdataStorer;

public class SdataImageDbStorer extends SdataStorer {

	private static final Logger log = LoggerFactory.getLogger("SdataImageDbStorer");
	
	private ImageMgDao dao = null;
	
	private FieldProcess fieldProcess;
	
	public SdataImageDbStorer(Configuration conf,RunState state){
		this.setConf(conf);
		this.state = state;
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.dao = new ImageMgDao(conf.get(Constants.SOURCE));;
		this.state = state;
		fieldProcess = new FieldProcess(conf);
	}
	
	@Override
	public void save(FetchDatum datum) throws Exception {
		String  absoluteFilePath = "";
		Long imageId = null;
		try {
			Map<String, Object> metadata = datum.getMetadata();
			String imageUrl = (String)metadata.get("imageUrl");
			String fileType = (String)metadata.get("fileType");
			String SiteType = (String)metadata.get("siteType");
			String imgId = (String)metadata.get("originalPhotoId");
			try {
				imageId = Long.valueOf(imgId);
			} catch (Exception e) {
				throw new NegligibleException("originalPhotoId is empty or abnormal!");
			}
			String filePath = getFilePath(imageId,SiteType,fileType);
			metadata.put("filePath",filePath);
			metadata.put(Constants.FETCH_TIME, new Date());
			metadata.put("uploadDate", getDate((String)metadata.get("uploadDate")));
			absoluteFilePath = saveImageFile(imageUrl, filePath);
			this.getImageSize(metadata, absoluteFilePath);
			metadata = fieldProcess.fieldReduce(metadata);
			dao.saveImage(metadata,fieldProcess);
		} catch (Exception e) {
			logFaileMessage(e);
			throw e;
		}
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
	
	public String getFilePath(Long imageId,String siteType,String fileType){
		//String siteType = image.getSiteType();
		if(DataWebSiteType.Flickr.equals(siteType)){
			// a type prefix for the directory name , "q" for keyword query , "b" for bounding box query.
			String prentStr = getPrent(imageId,4);
			return "/"+siteType +"/"+prentStr+"/"+imageId+"."+fileType;
		}else if(DataWebSiteType.Panoramio.equals(siteType)){
			String prentStr = getPrent(imageId,3);
			return "/"+siteType +"/"+prentStr+"/"+imageId+"."+fileType;
		}else{
			throw new RuntimeException("Unkown web site!");
		}
	}

	private String getPrent(Long imageId,int length) {
		String prentStr = null;
		String imageIdStr = StringUtils.valueOf(imageId);
		if(imageIdStr.length()>length){
			prentStr = imageIdStr.substring(0, length);
		}else{
			prentStr =StringUtils.valueOf(imageId/10000+1);
		}
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
			dao.deleteImage(imageId);
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
