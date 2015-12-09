/**
 * 
 */
package edu.buffalo.cse.cse486586.simpledynamo.message;

/**
 * @author roide
 *
 */
public enum MessageType {
    DATA_REQUEST,
    DATA_RESPONSE,
    NODE_JOIN_REQUEST,
    NODE_JOIN_RESPONSE,
    NODE_JOIN_NOTIFY,
    VALUE_INSERT_REQUEST,
    VALUE_QUERY_REQUEST,
    VALUE_QUERY_RESPONSE,
    VALUE_DELETE_REQUEST,
    ACK,
}
