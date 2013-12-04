package com.sdata.context.config;
/**
 * Constants
 * 
 * @author zhufb
 *
 */
public class Constants {

    public static final String FETCH_TIME = "fetdt";
    public static final String CREATE_TIME = "crtdt";
    public static final String SOURCE = "source";
    
    //weibo consumer and access info list
    public static final String WEIBO_FETCH_NUM = "FetchWeiboNum";
    public static final String WEIBO_ACCESS_TOKEN = "AccessToken";
    public static final String WEIBO_FETCH_PAGE_NUM = "FetchPageNum";
    public static final String WEIBO_STATUSES = "statuses";

    //mongdb pk
    public static final String OBJECT_ID = "_id";
    public static final String MONGO_SHKEY = "shkey";
    //tweet info list
    public static final String TWEET_ID = "id";
    public static final String TWEET_USER = "user";
    public static final String TWEET_RETWEETED= "retweeted_status";
    public static final String TWEET_RETWEETED_ID= "retid";
    public static final String TWEET_SOURCE_RETWEETED_COUNT= "srct";
    public static final String TWEET_LIST = "tweetsList";
    public static final String TWEET_TOPIC_ID = "topicsid";
    
    //topic info list
    public static final String TOPIC_ID = "id";
    public static final String TOPIC_NAME = "name";
    public static final String TOPIC_CONTENT = "name";
    public static final String TOPIC_URL = "url";
    public static final String TOPIC_COUNT = "count";
    public static final String TOPIC_DESCRIPTION = "description";
    public static final String TOPIC_IMG = "img";
    public static final String TOPIC_TWEETS = "tweetsList";
    public static final String TOPIC_STATE = "state";
    public static final String TOPIC_DATE_LIST = "tdl";
    public static final String TOPIC_STATE_START = "start";
    public static final String TOPIC_STATE_END = "end";
    public static final String TOPIC_LOCATION = "location";
  //  public static final String TOPIC_TWEETS_COUNT = "tweetsCount";
    
    //topic tweets
    public static final String TOPIC_TWEETS_LIST = "tl";
    public static final String TOPIC_TWEETS_FETCHED_LIST = "ftl";
    public static final String TOPIC_TWEETS_CURR_FETCHED_LIST = "cfl";
    
    //star info
    public static final String FAMOUS_ORDER = "order";
    public static final String FAMOUS_ID = "uid";
    public static final String FAMOUS_NAME = "name";
    public static final String FAMOUS_POPULAR = "popular";
    public static final String FAMOUS_HOMEPAGE = "homepage";
    public static final String FAMOUS_ACCOUNT = "account";
    public static final String FAMOUS_TWEETS = "tweetsList";
    public static final String FAMOUS_TWEETS_LIST = "ftl";
    public static final String FAMOUS_TWEETS_FLIST = "fList";
    public static final String FAMOUS_TWEET_ID = "tweetId";
    public static final String FAMOUS_NEWEST_TWEET = "newt";
    public static final String FAMOUS_NEWEST_TWEET_DATE = "newtd";
    public static final String FAMOUS_FRIENDS = "friends";
    public static final String FAMOUS_FRENDC = "frendc";
    public static final String FAMOUS_IDOLNUM = "idolnum";
    public static final String FAMOUS_FOLC = "folc";
    public static final String FAMOUS_FANSNUM = "fansnum";
    public static final String FAMOUS_STATC = "statc";
    public static final String FAMOUS_TWEETNUM = "tweetnum";
    public static final String FAMOUS_IDOLLIST = "idollist";
    
    //user info
    public static final String UID = "uid";
    public static final String USER_ID = "id";
    public static final String USER_LOCATION = "location";
    public static final String USER_STATUSES_COUNT = "statuses_count";
    public static final String USER_FRIENDS_COUNT = "friends_count";
    public static final String USER_FOLLOWERS_COUNT = "followers_count";
    public static final String USER_FAVOURITES_COUNT = "favourites_count";
    public static final String USER_DESCRIPTION = "description";
    public static final String USER = "user";
    public static final String USER_NAME = "name";
    public static final String USER_HOMEPAGE = "url";
    public static final String USER_LISTENS = "listens";
    public static final String USER_FOLLOWS = "follows";
    public static final String USER_FRIENDS = "users";
    public static final String COMMENTS = "comments";
    public static final String LIKES = "likes";
    public static final String DATA = "data";
    public static final String FROM = "from";
    public static final String AUTH_ID = "authid";
    public static final String AUTHOR = "author";
    public static final String AUTH_NAME = "authname";
    public static final String IMAGES = "images";
    public static final String CAPTION = "caption";
    public static final String TEXT = "text";
    public static final String CITY = "city";
    
    //user_relation
    public static final String USER_RELATION_FRIENDS = "friends";
    public static final String USER_RELATION_FOLLOWS = "follows";
    
    //sqlite queue info
    public static final String QUEUE_KEY = "key";
    public static final String QUEUE_NAME = "name";
    public static final String QUEUE_URL = "url";
    public static final String QUEUE_DEPTH = "depth";
    public static final String QUEUE_SEQUENCE_ID = "sequenceId";
    public static final String QUEUE_DISPOSE = "isdispose";
    public static final String QUEUE_STATUS = "status";
    //
    public static final int QUEUE_DEPTH_ROOT = 0;
    public static final String QUEUE_DEPTH_MAX = "crawlDepth";
    
    //user tweet info 
    public static final String DEL_YES = "1";
    public static final String DEL_NO = "0";

    //flag
    public static final String FLAG_YES = "1";
    public static final String FLAG_NO = "0";
    
    //user relation info
    public static final int FETCH_COUNT = 100;
    
    
    public static final String TENCENT_USER = "user";
    public static final String TENCENT_USER_INFO = "userinfo";
    public static final String TENCENT_USER_TWEETINFO = "tweetinfo";
    
    //flickr userRelation
    public static final String FLICKR_CONTACT = "contacts";
    public static final String FLICKR_CONTACTLIST = "contactList";
    public static final String FLICKR_IMAGELIST = "imageList";
    public static final String FLICKR_GROUPLIST = "groupList";
    public static final String FLICKR_MEMBERLIST = "memberList";
    public static final String FLICKR_GROUP = "group";
    public static final String FLICKR_IMAGE = "image";
    public static final String FLICKR_USER = "user";
    public static final String FLICKR_CURENT_FRIEND = "currentFriend";
    public static final String FLICKR_FAVORITES = "favorites";
    public static final String FLICKR_IS_COMMENTS_INDEPENDENCE = "isCommentsIndependece";
    

    public static final String SITE_TOP_AREA = "topArea";
    public static final String SITE_SECOND_AREA = "secondArea";
    public static final String SITE_MAX_DATUM = "maxDatum";
    
    //shop info
    public static final String SHOP_ID = "shopId";
    public static final String SHOP_NAME = "shopName";
    public static final String SHOP_ADDRESS = "address";
    public static final String SHOP_TASTE = "taste";
    
    //flickr userRelation parser type
    public static final String PARSER_TYPE_USER = "user";
    public static final String PARSER_TYPE_USER_GROUP = "usergroup";
    public static final String PARSER_TYPE_USERLIST = "userlist";
    public static final String PARSER_TYPE_IMAGELIST = "imagelist";
    public static final String PARSER_TYPE_GROUPLIST = "grouplist";
    public static final String PARSER_TYPE_MEMBERLIST = "memberlist";
    public static final String KEYWORD_QUERY_OVER = "keywordQueryOver";
    
    //reviews
    public static final String REVIEWS = "reviews";
    public static final String REVIEWS_NUM = "revnum";
    public static final String IMAGES_NUM = "imgnum";
    public static final String CYCLE = "cycle";
    
    //reviews
    public static final String COMTS = "comts";
    public static final String COMTS_ID = "comtsId";
    
    //address info
    public static final String ADDRESS_NAME = "name";
    public static final String ADDRESS = "address";
    public static final String ADDRESS_POSTCODE = "postcode";
    
    //tencent famous
    public static final String TENCENT_FAMOUS_ORDER = "order";
   // public static final String TENCENT_FAMOUS_TOPDATA = "topData";
    public static final String TENCENT_FAMOUS_NAME = "name";
    public static final String TENCENT_FAMOUS_HEAD = "head";
    public static final String TENCENT_FAMOUS_NICK = "nick";
    public static final String TENCENT_FAMOUS_PRINT = "pint";
    public static final String TENCENT_FAMOUS_USERURL = "url";
    
}
