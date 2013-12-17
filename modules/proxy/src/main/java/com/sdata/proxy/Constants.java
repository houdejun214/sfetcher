package com.sdata.proxy;
/**
 * Constants
 * 
 * @author zhufb
 *
 */
public class Constants {

    public static final String FETCH_TIME = "fet_time";
    public static final String PUB_TIME = "pub_time";
    
    public static final String UID = "uid";
    
    //data must have
    public static final String DATA_PK = "_id";//数据标签-来自xx源
    public static final String DATA_INDEX_COLUMN = "_index";//数据标签-来自xx源
    public static final String DATA_ID = "id";//数据标签-来自xx源
    public static final String DATA_URL = "url";//数据标签-来自xx源
    public static final String DATA_USER = "user";//数据标签-来自xx源

    //data tags
    public static final String DATA_TAGS_FROM_SOURCE = "dtf_s";//数据标签-来自xx源
    public static final String DATA_TAGS_FROM_PARAM_VALUE = "dtf_pv";//数据标签-来自xx关键字
    public static final String DATA_TAGS_FROM_PARAM_TYPE = "dtf_pt";//数据标签-来自关键字类型
    public static final String DATA_TAGS_FROM_OBJECT_ID = "dtf_oid";//数据标签-来自关Object id
}
