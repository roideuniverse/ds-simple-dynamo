package edu.buffalo.cse.cse486586.simpledynamo.action;

import java.util.Map;

import edu.buffalo.cse.cse486586.simpledynamo.Util;
import android.database.MatrixCursor;

public class ActionClientQuery extends Action {

    Map<String,String> mQueryResult;
    public ActionClientQuery(String key) {
        super(key);
    }

    @Override
    public ActionType getType() {
        return ActionType.CLIENT_QUERY;
    }
    
    public void setQueryResult(Map<String,String> res) {
        mQueryResult = res;
    }
    
    public Map<String,String> getQueryResultAsMap() {
        return mQueryResult;
    }
    
    public MatrixCursor getQueryResultAsCursor() {
        if(mQueryResult == null)
            return null;
        return Util.MapToCursor(mQueryResult);
    }

    @Override
    public Action call() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }
    

}
