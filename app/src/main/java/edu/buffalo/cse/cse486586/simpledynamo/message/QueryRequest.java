package edu.buffalo.cse.cse486586.simpledynamo.message;

import java.util.Map;
import java.util.Random;

import edu.buffalo.cse.cse486586.simpledynamo.Util;

public class QueryRequest implements IMessage {
    private int mDestination;
    private String mNodeId;
    private String mNodeName;
    private int mHost;
    private String mQueryKey;
    private boolean mHasPayload;
    private Map<String,String> mPayload;
    private String mMsgId;
    
    public QueryRequest() {
        Random r = new Random();
        r.setSeed(System.currentTimeMillis() + r.nextLong());
        this.mMsgId = Util.genHash(""+System.currentTimeMillis() + r.nextLong()) + r.nextLong() ;
    }

    @Override
    public MessageType getType() {
        return MessageType.VALUE_QUERY_REQUEST;
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
    
    public void setQueryString(String queryKey) {
        mQueryKey = queryKey;
    }
    
    public String getQueryKey() {
        return mQueryKey;
    }
    
    public boolean hasPayload() {
        return mHasPayload;
    }
    
    public void setPayload(Map<String,String> payload) {
        if(payload != null && !payload.isEmpty()) 
            mHasPayload = true;
        mPayload = payload;
    }
    
    public Map<String,String> getPayload() {
        return mPayload;
    }
    @Override
    public String getId() {
        return mMsgId;
    }

}
