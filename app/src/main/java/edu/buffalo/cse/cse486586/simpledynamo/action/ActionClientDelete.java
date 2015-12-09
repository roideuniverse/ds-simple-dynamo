package edu.buffalo.cse.cse486586.simpledynamo.action;

public class ActionClientDelete extends Action {

    public ActionClientDelete(String key) {
        super(key);
    }

    @Override
    public ActionType getType() {
        return ActionType.CLIENT_DELETE;
    }

    @Override
    public Action call() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

}
