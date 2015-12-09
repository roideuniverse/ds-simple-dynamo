package edu.buffalo.cse.cse486586.simpledynamo.message;

import java.util.ArrayList;
import java.util.Random;

import edu.buffalo.cse.cse486586.simpledynamo.Util;

public class DataRequest implements IMessage {

    private int mDestination;
    private String mNodeId;
    private String mNodeName;
    private int mHost;
    private ArrayList<String> mRequestDataNodeName;
    
    private String mMsgId;
    
    public DataRequest() {
        Random r = new Random();
        r.setSeed(System.currentTimeMillis() + r.nextLong());
        this.mMsgId = Util.genHash(""+System.currentTimeMillis() + r.nextLong()) + r.nextLong() ;
        mRequestDataNodeName = new ArrayList<String>();
    }

    @Override
    public MessageType getType() {
        return MessageType.DATA_REQUEST;
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
    
    public void addNodeName(String nodeName) {
        mRequestDataNodeName.add(nodeName);
    }
    
    public void addNodeName(ArrayList<String> nodeNameList) {
        mRequestDataNodeName.addAll(nodeNameList);
    }
    
    public ArrayList<String> getRequestDataNodeName() {
        return mRequestDataNodeName;
    }

}
