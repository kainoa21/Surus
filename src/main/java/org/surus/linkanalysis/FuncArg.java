/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

import org.surus.linkanalysis.FuncArgFactory.FuncArgType;
import java.util.Map;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 *
 * @author jasonr
 */
public interface FuncArg {
    public FuncArgType getArgType();
    public Object getValue(Map<String, Map<Integer, Tuple>> partitionMatch, Schema inputSchema) throws ExecException;
    public byte getDataType();
    public FieldSchema getSchema(Schema inputSchema) throws FrontendException;
}
