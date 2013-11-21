package com.sdata.component.data.storer;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.FileUtils;
import com.lakeside.core.utils.PathUtils;
import com.lakeside.core.utils.SimpleImageInfo;
import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.SdataStandardStorer;

/**
 * download image file to local and get image's height and width
 * @author qiumm
 *
 */
public class SdataImagesStorer extends SdataStandardStorer{
	private static final Logger log = LoggerFactory.getLogger("SdataImagesStorer");
	
	public SdataImagesStorer(Configuration conf, RunState state) {
		super(conf, state);
	}
	
	@Override
	public void save(FetchDatum datum) {
		Map<String, Object> metadata = datum.getMetadata();
		String imageUrl = StringUtils.valueOf(metadata.get("imageUrl"));
		String filePath = getFilePath(getOriginalPhotoId(imageUrl));
		Map<String, Object> imgurl= new HashMap<String, Object>();
		imgurl.put("path",filePath);
		imgurl.put("url", imageUrl);
		metadata.put("imgurl",imgurl);
		//TODO DOES NOT SAVE IMAGE 
//		String absoluteFilePath = null;
//		try {
//			absoluteFilePath = saveImageFile(imageUrl, filePath);
//		} catch (MalformedURLException e) {
//			log.info(e.getMessage());
//			this.deleteOneImageForException(absoluteFilePath);
//			return;
//		} catch (IOException e) {
//			log.info(e.getMessage());
//			this.deleteOneImageForException(absoluteFilePath);
//			return;
//		}
//		if(absoluteFilePath!=null){
//			this.getImageSize(metadata, absoluteFilePath);
//		}
		super.save(metadata);
	}
	
	private String getOriginalPhotoId(String url){
		if(url.endsWith("/")){
			url = url.substring(0,url.length()-1);
		}
		int start = url.lastIndexOf("/");
		String photoId = url.substring(start+1);
		return photoId;
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
	
	protected void deleteOneImageForException(String filePath){
		try {
			File file=new File(filePath);
			if(file.exists()){
				boolean delete = file.delete();
				if(!delete){
					throw new RuntimeException("delete file ["+filePath+"] failed");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

}
