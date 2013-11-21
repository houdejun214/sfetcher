package com.sdata.component.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;

import com.lakeside.core.utils.FileUtils;
import com.lakeside.core.utils.StringUtils;

public class YouTubeVideoDownloadUtils {
	
	// something like [http://][*].youtube.[cc|to|pl|ev|do|ma|in]/   the last / is for marking the end of host, it does not belong to the hostpart
	public static final String szYTHOSTREGEX = "^((H|h)(T|t)(T|t)(P|p)(S|s)?://)?(.*)\\.(Y|y)(O|o)(U|u)(T|t)(U|u)(B|b)(E|e)\\..{2,5}/";
		
	public static void main(String[] args){
		YouTubeVideoDownloadUtils youtube = new YouTubeVideoDownloadUtils();
		String id = "ft9JNRKSHBc";
		String url="http://www.youtube.com/watch?v=ft9JNRKSHBc";
		String dir="F:\\Videodataset\\YoutubeforConceptSequence\\videos";
		String videoType = "18";
		youtube.download(url, dir, id,id,videoType);
	}
	
	public Map<String ,Object> download(String url,String dir,String id,String name,String videoType){
		Map<String ,Object> result = new HashMap<String ,Object>();
		boolean isDownload = false;
		String errorReason = null;
		// stop recursion
		if (StringUtils.isEmpty(url)){
			isDownload = false;
			errorReason = "url is empty.";
			result.put("isDownload", isDownload);
	        result.put("errorReason", errorReason);
	       	return result;
		} 
		HttpClient	httpclient = new DefaultHttpClient();
		httpclient.getParams().setParameter(ClientPNames.COOKIE_POLICY, CookiePolicy.BEST_MATCH);
		
		HttpGet	httpget = new HttpGet( getURI(url) );	
		HttpHost target = null;
		if (url.toLowerCase().startsWith("https")){
			target = new HttpHost( getHost(url), 443, "https" );
		}else{
			target = new HttpHost( getHost(url), 80, "http" );
		}
		HttpResponse response = null;
		HttpContext	localContext = null;
		try {
			response = httpclient.execute(target,httpget,localContext);
		} catch (ClientProtocolException cpe) {
			isDownload = false;
			errorReason = "url can't view:"+url;
		} catch (UnknownHostException uhe) {
			isDownload = false;
			errorReason = "url can't view:"+url;
		} catch (IOException ioe) {
			isDownload = false;
			errorReason = "url can't view:"+url;
		} catch (IllegalStateException ise) {
			isDownload = false;
			errorReason = "url can't view:"+url;
		}
		
		HttpEntity entity = null;
        try {
            entity = response.getEntity();
        } catch (NullPointerException npe) {
        	isDownload = false;
        	errorReason = "url can't view:"+url;
        }
        BufferedReader		textreader = null;
        String 				sContentType = null;
        
        // try to read HTTP response body
        if (entity != null) {
			try {
				if (response.getFirstHeader("Content-Type").getValue().toLowerCase().matches("^text/html(.*)")){
					textreader = new BufferedReader(new InputStreamReader(entity.getContent()));
				}else{
					isDownload = false;
					errorReason = "url view exception is not html:"+url;
				}
			} catch (IllegalStateException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
            try {
            	sContentType = response.getFirstHeader("Content-Type").getValue().toLowerCase();
            	
            	if (sContentType.matches("^text/html(.*)")) {
            		try {
						isDownload = getHTMLInfo(dir, id, isDownload, textreader,name,videoType);
					}catch (RuntimeException e) {
						isDownload = false;
						errorReason = e.getMessage();
					} 
            	}  else { // content-type is not video/
            		isDownload = false;
            		errorReason = "url view exception is not html:"+url;
            	}
            } catch (RuntimeException ex) {
            	isDownload = false;
        		errorReason = ex.toString();
    		} catch (IOException ex) {
            	try {
					throw ex;
				} catch (Exception e) {
					e.printStackTrace();
				}
    		} catch (Exception ex) {
                try {
					throw ex;
				} catch (Exception e) {
					e.printStackTrace();
				}
            }finally {
    		    try {
    				textreader.close();
    			} catch (Exception e) {
    			}
    		} // try
        } //if (entity != null)
        httpclient.getConnectionManager().shutdown();
        result.put("isDownload", isDownload);
        result.put("errorReason", errorReason);
       	return result;
	}

	private boolean getHTMLInfo(String dir, String id, boolean isDownload,
			BufferedReader textreader,String name,String videoType) throws IOException {
		String sline = "";
		boolean isFindSline = false;
		try {
			while (sline != null) {
				sline = textreader.readLine();
				if(sline!=null && sline.matches("(.*)\"url_encoded_fmt_stream_map\":(.*)")){
					isFindSline = true;
					sline = sline.replaceFirst(".*\"url_encoded_fmt_stream_map\": \"", "").replaceFirst("\".*", "").replace("%25","%").replace("\\u0026", "&").replace("\\", "");
					String[] urlStrings = sline.split(",");
					boolean isfindurl = false;
					for (String urlString : urlStrings) {
						String[] fmtUrlPair = urlString.split("&itag="); 
						if(fmtUrlPair.length==2){
							String itag = fmtUrlPair[1];
							if(itag.equals(videoType)){
								fmtUrlPair[0] = fmtUrlPair[0].replaceFirst("url=http%3A%2F%2F", "http://"); // 2011-08-20 key-value exchanged
								fmtUrlPair[0] = fmtUrlPair[0].replaceAll("%3F","?").replaceAll("%2F", "/").replaceAll("%3D","=").replaceAll("%26", "&");
								fmtUrlPair[0] = fmtUrlPair[0].replaceFirst("&quality=.*", "");
								String url_video = fmtUrlPair[0];
								isfindurl = true;
								isDownload = downloadVideo(url_video,dir,id,name,videoType);
								break;
							}
						}else{
							int i = urlString.indexOf("&");
							String itag = urlString.substring(5	, i);
							if(itag.equals(videoType)){
								String url_video = urlString.substring(i+1);
								url_video = url_video.replaceFirst("url=http%3A%2F%2F", "http://"); // 2011-08-20 key-value exchanged
								url_video = url_video.replaceAll("%3F","?").replaceAll("%2F", "/").replaceAll("%3D","=").replaceAll("%26", "&");
								url_video = url_video.replaceFirst("&quality=.*", "");
								isfindurl = true;
								isDownload = downloadVideo(url_video,dir,id,name,videoType);
								break;
							}
						}
					}
					if(!isfindurl){
						throw new RuntimeException("can't find the url for filetype MP4.");
						
					}
					
				}
			}
			if(!isFindSline){
				throw new RuntimeException("can't find the url for download stream.");
				
			}
		} catch (RuntimeException e) {
			throw e;
		}
		return isDownload;
	}
	
	private boolean downloadVideo(String url,String dir,String id,String name,String videoType){
		boolean isDownload = false;
		// stop recursion
		try {
			if (url.equals("")){
				return(false);
			} 
		} catch (NullPointerException npe) {
			return(false);
		}
		HttpClient	httpclient = new DefaultHttpClient();
		httpclient.getParams().setParameter(ClientPNames.COOKIE_POLICY, CookiePolicy.BEST_MATCH);
		
		HttpGet	httpget = new HttpGet( getURI(url) );	
		HttpHost target = null;
		if (url.toLowerCase().startsWith("https")){
			target = new HttpHost( getHost(url), 443, "https" );
		}else{
			target = new HttpHost( getHost(url), 80, "http" );
		}
		HttpResponse response = null;
		HttpContext	localContext = null;
		try {
			response = httpclient.execute(target,httpget,localContext);
		} catch (ClientProtocolException cpe) {
			throw new RuntimeException("the video download url can't view:"+url);
		} catch (UnknownHostException uhe) {
			throw new RuntimeException("the video download url can't view:"+url);
		} catch (IOException ioe) {
			throw new RuntimeException("the video download url can't view:"+url);
		} catch (IllegalStateException ise) {
			throw new RuntimeException("the video download url can't view:"+url);
		}
		
		HttpEntity entity = null;
        try {
            entity = response.getEntity();
        } catch (NullPointerException npe) {
        	throw new RuntimeException("the video download url can't view:"+url);
        }
        BufferedInputStream binaryreader = null;
        String 				sContentType = null;
        // try to read HTTP response body
        if (entity != null) {
			try {
				binaryreader = new BufferedInputStream( entity.getContent());
			} catch (IllegalStateException e1) {
				throw new RuntimeException("the response is not a stream for download");
			} catch (IOException e1) {
				throw new RuntimeException("internet IOException");
			}
            try {
            	sContentType = response.getFirstHeader("Content-Type").getValue().toLowerCase();
            	
            	if (sContentType.matches("video/(.)*")) {
            		isDownload = saveVideo(dir, id, httpclient, response,binaryreader, sContentType,name);
            	} else { // content-type is not video/
            		String reason = response.getStatusLine().getReasonPhrase();
            		throw new RuntimeException("the video can't download because:"+reason);
            	}
            } catch (RuntimeException ex) {
            	throw ex;
    		} catch (IOException ex) {
            	throw new RuntimeException("the video can't download because IOException");
    		} catch (Exception ex) {
				try {
					throw ex;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
        } //if (entity != null)
        httpclient.getConnectionManager().shutdown();
       	return isDownload;
	}
	

	private boolean saveVideo(String dir, String id, HttpClient httpclient,
			HttpResponse response,BufferedInputStream binaryreader, String sContentType,String name)
			throws FileNotFoundException, IOException {
		FileOutputStream fos = null;
		boolean downloadOK = true;
		String sfilename = id;
		File f;
		String sdirectorychoosed = dir;
		f = new File(sdirectorychoosed, sfilename.concat(".").concat(sContentType.replaceFirst("video/", "").replaceAll("x-", "")));
		try {
			if(f.exists() && !f.canWrite()){
				return false;
			}
			FileUtils.insureFileDirectory(f.getAbsolutePath());
			Long iBytesReadSum = (long) 0;
			Long iPercentage = (long) -1;
			Long iBytesMax = Long.parseLong(response.getFirstHeader("Content-Length").getValue());
			fos = new FileOutputStream(f,false);
			
			byte[] bytes = new byte[4096];
			Integer iBytesRead = 1;
			
			// adjust blocks of percentage to output - larger files are shown with smaller pieces
			Integer iblocks = 10; if (iBytesMax>20*1024*1024) iblocks=4; if (iBytesMax>32*1024*1024) iblocks=2; if (iBytesMax>56*1024*1024) iblocks=1;
			while (iBytesRead>0) {
				iBytesRead = binaryreader.read(bytes);
				iBytesReadSum += iBytesRead;
				// drop a line every x% of the download 
				if ( (((iBytesReadSum*100/iBytesMax) / iblocks) * iblocks) > iPercentage ) {
					iPercentage = (((iBytesReadSum*100/iBytesMax) / iblocks) * iblocks);
				}
				
				try {fos.write(bytes,0,iBytesRead);} catch (IndexOutOfBoundsException ioob) {}
			} // while
			if (iBytesReadSum<iBytesMax) {
				try {
					// this part is especially for our M$-Windows users because of the different behavior of File.renameTo() in contrast to non-windows
					// see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6213298  and others
					// even with Java 1.6.0_22 the renameTo() does not work directly on M$-Windows! 
					fos.close();
				} catch (Exception e) {
				}
//            				System.gc(); // we don't have to do this but to be sure the file handle gets released we do a thread sleep 
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
				}
				// this part runs on *ix platforms without closing the FileOutputStream explicitly
				httpclient.getConnectionManager().shutdown(); // otherwise binaryreader.close() would cause the entire datastream to be transmitted 
			}
		
		} catch (FileNotFoundException fnfe) {
			downloadOK = false;
		} catch (IOException ioe) {
			downloadOK = false;
			f.delete();
			throw new RuntimeException("can't download the video because IOException.");
		} finally {
			try {
				fos.close();
			} catch (Exception e) {
			}
		    try {
				binaryreader.close();
			} catch (Exception e) {
			}
		} // try
		return downloadOK;
	}
	
	String getURI(String sURL) {
		String suri = "/".concat(sURL.replaceFirst(szYTHOSTREGEX, ""));
		return(suri);
	}
	
	String getHost(String sURL) {
		String shost = sURL.replaceFirst(szYTHOSTREGEX, "");
		shost = sURL.substring(0, sURL.length()-shost.length());
		shost = shost.toLowerCase().replaceFirst("http[s]?://", "").replaceAll("/", "");
		return(shost);
	}
}
