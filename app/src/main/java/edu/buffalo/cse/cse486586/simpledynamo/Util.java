/**
 * 
 */
package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import edu.buffalo.cse.cse486586.simpledynamo.message.IMessage;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.telephony.TelephonyManager;
import android.util.Log;

/**
 * @author roide
 *
 */
public class Util {
    private static final String TAG = Util.class.getSimpleName();
    /**
     * 
     */
    private Util() {
    }
    
    public static String getEmulatorName(Context context) {
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        //Log.d(TAG, "line no=" + tel.getLine1Number());
        String emulatorNo = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        //Log.d(TAG, "emulatorNo=" + emulatorNo  );
        return emulatorNo;
    }
    
    public static Map<String,String> CursorToMap(Cursor cursor) {
        HashMap<String,String> map = new HashMap<String, String>();
        
        if (cursor.moveToFirst()) {
            while (!cursor.isAfterLast()) {
                String key = cursor.getString(cursor.getColumnIndex(Constants.KEY));
                String val = cursor.getString(cursor.getColumnIndex(Constants.VALUE));
                map.put(key, val);
                cursor.moveToNext();
            }
        }
        cursor.close();
        return map;
    }
    
    public static MatrixCursor MapToCursor(Map<String,String> map) {
        //Log.d(TAG, "map:" + map);
        String[] columns = {
                Constants.KEY, Constants.VALUE
        };
        MatrixCursor matrixCursor = new MatrixCursor(columns);
        Set<String> keySet = map.keySet();
        for(String key:keySet) {
            String val = map.get(key);
            matrixCursor.addRow(new String[] {
                    key, val
            });
        }
        return matrixCursor;
    }
    
    public static String genHash(String input) {
        MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");

            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
    
    public static String getSuccessorId(String nodeName) {
        //Log.d(TAG, "getSuccessorId()::" + nodeName);
        ArrayList<String> nodeList = Constants.NODE_LIST ;
        String nodeId = Util.genHash(nodeName);
        String succId = null;
        if(nodeList.contains(nodeId)) {
            int index = nodeList.indexOf(nodeId);
            if(index != nodeList.size()-1) {
                succId = nodeList.get(index+1);
            } else {
                succId = nodeList.get(0);
            }
            return succId;
        }
        return null;
    }
    
    public static String getPredecessorId(String nodeName) {
        //Log.d(TAG, "getPredecessorId()::" + nodeName);
        ArrayList<String> nodeList = Constants.NODE_LIST ;
        String nodeId = Util.genHash(nodeName);
        String predId = null;
        if(nodeList.contains(nodeId)) {
            int index = nodeList.indexOf(nodeId);
            if(index != 0) {
                predId = nodeList.get(index-1);
            } else {
                predId = nodeList.get(nodeList.size()-1);
            }
            return predId;
        }
        return null;
    }
    
    public static String getSuccessorName(String nodeName) {
        String succId = getSuccessorId(nodeName);
        return Constants.MAP_NODEID_NAME.get(succId);
    }
    
    public static String getPredecessorName(String nodeName) {
        String predId = getPredecessorId(nodeName);
        return Constants.MAP_NODEID_NAME.get(predId);
    }
    
    public static String getParentName(String key) {
        for(String nodeName:Constants.MAP_NODENAME_PORT.keySet()) {
            String localId = Util.genHash(nodeName);
            if(iskeyLocal(key, localId)) {
                return nodeName;
            }
        }
        return null;
    }
    
    static boolean iskeyLocal(String key, String localNodeId) {
        //Log.d(TAG, "isKeyLocal::key="+ key + " local=" + localNodeId );
        String predecessorId = Util.getPredecessorId(Constants.MAP_NODEID_NAME.get(localNodeId));
        //Log.d(TAG, "isKeyLocal::key="+ key + " pred=" + predecessorId);
        
        String hKey = Util.genHash(key);
        int compToCurr = hKey.compareTo(localNodeId);
        int compToPred = hKey.compareTo(predecessorId);

        /*
         * key equal to node, so should be assigned here.
         */
        if (compToCurr == 0) {
            //Log.d(TAG, "key equall to node::true");
            return true;
        }

        if (compToPred > 0 && compToCurr < 0) {
            //Log.d(TAG, "key>pred & key < curr::true");
            return true;
        }
        
        if (compToPred > 0 && compToCurr > 0) {
            int comp = predecessorId.compareTo(localNodeId);
            if (comp > 0) {
                //Log.d(TAG, "key>pred & key>cur & pred>curr::true");
                return true;
            }
        }

        if (compToPred < 0 && compToCurr < 0) {
            int comp = predecessorId.compareTo(localNodeId);
            if (comp > 0) {
                //Log.d(TAG, "key<pred & key<curr & pred>curr::inserting local");
                return true;
            }
        }
        return false;
    }
    
    public static boolean isNodeSuccessor(String key, String localNodeId) {
        String parentName = Util.getParentName(key);
        String succId = Util.getSuccessorId(parentName);
        if(succId.equals(localNodeId)) {
            return true;
        }
        return false;
    }
    
    public static boolean isNodeSecondSuccessor(String key, String localNodeId) {
        String parentName = Util.getParentName(key);
        String succName = Util.getSuccessorName(parentName);
        String secondSuccId = Util.getSuccessorId(succName);
        if(secondSuccId.equals(localNodeId)) {
            return true;
        }
        return false;
    }
    
    static int getPort(String nodeName) {
        return Constants.MAP_NODENAME_PORT.get(nodeName);
    }
    
    static ContentValues createCV(String key, String value) {
        ContentValues cv = new ContentValues();
        cv.put(Constants.KEY, key);
        cv.put(Constants.VALUE, value);
        
        return cv;
    }
    
    static Map<String,String> createMap(String key, String value) {
        HashMap<String,String> m = new HashMap<String, String>();
        m.put(key, value);
        return m;
    }

}
