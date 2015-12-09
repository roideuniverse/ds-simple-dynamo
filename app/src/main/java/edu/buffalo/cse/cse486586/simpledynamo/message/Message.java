/**
 * 
 */
package edu.buffalo.cse.cse486586.simpledynamo.message;

import java.util.Random;

import edu.buffalo.cse.cse486586.simpledynamo.Util;

/**
 * @author roide
 *
 */
public class Message implements IMessage {

    private MessageType mType;
    private String mNodeId;
    private String mNodeName;
    private int mDestination;
    private int mHost;
    private String mMsgId;
    
    
    /**
     * 
     */
    public Message(MessageType type) {
        mType = type;
        Random r = new Random();
        r.setSeed(System.currentTimeMillis() + r.nextLong());
        this.mMsgId = Util.genHash(""+System.currentTimeMillis() + r.nextLong()) + r.nextLong() ;
    }

    /* (non-Javadoc)
     * @see edu.buffalo.cse.cse486586.simpledht.message.IMessage#getType()
     */
    @Override
    public MessageType getType() {
        return mType;
    }
    
    public void setNodeId(String nId) {
        mNodeId = nId;
    }
    
    public String getNodeId() {
        return mNodeId;
    }

    public int getDestination() {
        return mDestination;
    }

    public void setDestination(int mDestination) {
        this.mDestination = mDestination;
    }

    @Override
    public void setHost(int host) {
        mHost = host;
    }

    @Override
    public int getHost() {
        return mHost;
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
    public String getId() {
        return mMsgId;
    }

}
