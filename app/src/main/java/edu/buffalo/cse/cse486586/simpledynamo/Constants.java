/**
 * 
 */
package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author roide
 *
 */
public class Constants {

    /**
     * 
     */
    private Constants() {
    }
    
    public static final int SOCKET_TIMEOUT_LESS = 300*2;
    public static final int SOCKET_TIMEOUT_AVERAGE = 500*2;
    public static final int SOCKET_TIMEOUT_HIGH = 740*2;
    public static final int SLEEP_TIME = 1000;
    public static final String NODE_NAME1 = "5554";
    public static final String NODE_NAME2 = "5556";
    public static final String NODE_NAME3 = "5558";
    public static final String NODE_NAME4 = "5560";
    public static final String NODE_NAME5 = "5562";
    
    public static final int SERVER_PORT = 10000;
    public static final int MASTER_NODE_PORT=11108;
    
    public static final int RESULT_SUCCESS = 101;
    public static final int RESULT_FAILURE = 100;
    
    public static final HashMap<String,Integer> MAP_NODENAME_PORT = new HashMap<String, Integer>();
    public static final HashMap<String,String> MAP_NODEID_NAME = new HashMap<String, String>();
    public static final ArrayList<String> NODE_LIST = new ArrayList<String>();
    
    public static final String KEY = "key";
    public static final String VALUE = "value";
    public static final String VERSION = "version";
    
    static {
        MAP_NODENAME_PORT.put(NODE_NAME1, 11108);
        MAP_NODENAME_PORT.put(NODE_NAME2, 11112);
        MAP_NODENAME_PORT.put(NODE_NAME3, 11116);
        MAP_NODENAME_PORT.put(NODE_NAME4, 11120);
        MAP_NODENAME_PORT.put(NODE_NAME5, 11124);
                
        for(String key:MAP_NODENAME_PORT.keySet()) {
            String hash = Util.genHash(key); 
            NODE_LIST.add(hash);
            MAP_NODEID_NAME.put(hash, key);
        }
        Collections.sort(NODE_LIST);
    }
    

}
