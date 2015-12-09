package edu.buffalo.cse.cse486586.simpledynamo.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import edu.buffalo.cse.cse486586.simpledynamo.Util;

public class DataResponse implements IMessage {

    private int mDestination;
    private String mNodeId;
    private String mNodeName;
    private int mHost;
    
    private String mMsgId;
    private HashMap<String, ArrayList<String>> mPayload;
    private boolean hasPayload;
    
    public DataResponse() {
        Random r = new Random();
        r.setSeed(System.currentTimeMillis() + r.nextLong());
        this.mMsgId = Util.genHash(""+System.currentTimeMillis() + r.nextLong()) + r.nextLong() ;
        
        mPayload = new HashMap<String,ArrayList<String>>();
    }

    @Override
    public MessageType getType() {
        return MessageType.DATA_RESPONSE;
    }

    @Override
    public String getId() {
        return mMsgId;
    }

    @Override
    public void setDestination(int destination) {
        mDestination = destination;
    }

    @Override
    public int getDestination() {
        return mDestination;
    }

    @Override
    public void setNodeId(String nodeId) {
        mNodeId = nodeId;
    }

    @Override
    public String getNodeId() {
        return mNodeId;
    }

    @Override
    public void setNodeName(String name) {
        mNodeName = name;
    }

    @Override
    public String getNodeName() {
        return mNodeName;
    }

    @Override
    public void setHost(int host) {
        mHost = host;
    }

    @Override
    public int getHost() {
        return mHost;
    }
    
    public void setPayload(Map<String,ArrayList<String>> payload) {
        mPayload.putAll(payload);
        hasPayload = true;
    }
    
    public Map<String,ArrayList<String>> getPayload() {
        return mPayload;
    }
    
    public boolean hasPayload() {
        return hasPayload;
    }
}
