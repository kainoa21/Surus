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
public class FuncArgString implements FuncArg {
    
    private final String value;
    
    public FuncArgString(String arg) {
        this.value = arg;
    }

    @Override
    public FuncArgFactory.FuncArgType getArgType() {
        return FuncArgType.STRING;
    }

    @Override
    public String getValue(Map<String, Map<Integer, Tuple>> matchGroup, Schema inputSchema) throws ExecException {
        return value;
    }
    
    @Override
    public byte getDataType() {
        return DataType.CHARARRAY;
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
        FuncArgString other = (FuncArgString) obj;
        try {
            return this.value.equals(other.getValue(null, null));
        } catch (Exception e) {
            return false;
        }
        
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + (this.value != null ? this.value.hashCode() : 0);
        return hash;
    }
    
}
