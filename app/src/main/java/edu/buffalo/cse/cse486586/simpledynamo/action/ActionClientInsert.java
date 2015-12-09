package edu.buffalo.cse.cse486586.simpledynamo.action;

public class ActionClientInsert extends Action {
    
    private String mValue;
    
    public ActionClientInsert(String key, String value) {
        super(key);
        mValue = value;
    }

    @Override
    public ActionType getType() {
        return ActionType.CLIENT_INSERT;
    }
    
    public String getValue() {
        return mValue;
    }

    @Override
    public Action call() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

}
