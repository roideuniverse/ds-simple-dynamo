/**
 * 
 */
package edu.buffalo.cse.cse486586.simpledynamo.action;

import java.util.concurrent.Callable;

/**
 * @author roide
 *
 */
public abstract class Action implements IAction, Callable<Action>{
    
    private int mSeqNo;
    private String mKey;
    
    public Action() {
        
    }
    /**
     * 
     */
    public Action(String key) {
        mKey = key;
    }
    
    @Override
    public abstract ActionType getType();

    public void setSeqNo(int num) {
        mSeqNo = num;
    }
    
    public int getSeqNo() {
        return mSeqNo;
    }
    
    public String getKey() {
        return mKey;
    }
}
