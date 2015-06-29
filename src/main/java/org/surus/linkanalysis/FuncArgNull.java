/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

import org.surus.linkanalysis.FuncArgFactory.FuncArgType;
import java.util.Map;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 *
 * @author jasonr
 */
public class FuncArgNull implements FuncArg {
    
    public FuncArgNull() {
    }

    @Override
    public FuncArgFactory.FuncArgType getArgType() {
        return FuncArgType.NULL;
    }

    @Override
    public String getValue(Map<String, Map<Integer, Tuple>> matchGroup, Schema inputSchema) throws ExecException {
        return null;
    }
    
    @Override
    public byte getDataType() {
        return DataType.BYTEARRAY;
    }
    
    @Override
    public FieldSchema getSchema(Schema inputSchema) {
        return new FieldSchema(null, this.getDataType());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return true;        
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash;
        return hash;
    }
    
}
