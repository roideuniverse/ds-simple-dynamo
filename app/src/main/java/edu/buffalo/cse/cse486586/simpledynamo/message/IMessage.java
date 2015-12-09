/**
 * 
 */
package edu.buffalo.cse.cse486586.simpledynamo.message;

import java.io.Serializable;

/**
 * @author roide
 *
 */
public interface IMessage extends Serializable {
    public MessageType getType();
    public String getId();
    
    public void setDestination(int destination);
    public int getDestination();
    
    public void setNodeId(String nodeId);
    public String getNodeId();
    
    public void setNodeName(String name);
    public String getNodeName();
    
    public void setHost(int host);
    public int getHost();
}
