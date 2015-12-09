package edu.buffalo.cse.cse486586.simpledynamo.action;

public interface IAction {
    public ActionType getType();
    public int getSeqNo();
    public void setSeqNo(int num);
}
