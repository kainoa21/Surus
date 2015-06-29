/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

import org.surus.linkanalysis.FuncArgFactory.FuncArgType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 *
 * @author jasonr
 */
public class FuncArgColumn implements FuncArg {
    
    private static final BagFactory bagFactory = BagFactory.getInstance();
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();
    
    private final String columnName;
    private final String[] symbols;
    
    public FuncArgColumn(String columnName, String[] symbols) {
        this.columnName = columnName;
        this.symbols = symbols;
    }

    @Override
    public FuncArgFactory.FuncArgType getArgType() {
        return FuncArgType.STRING;
    }
    
    public String getColumnName() {
        return this.columnName;
    }
    
    public String[] getSymbols() {
        return this.symbols;
    }

    @Override
    public DataBag getValue(Map<String, Map<Integer, Tuple>> matchGroup, Schema inputSchema) throws ExecException {
        Map<Integer, Tuple> selectedTuples = new HashMap<Integer, Tuple>();
        
        for (String alias : this.symbols) {
            
            if (matchGroup.containsKey(alias)) {
                selectedTuples.putAll((Map<Integer, Tuple>)matchGroup.get(alias));
            }
        }
        
        // Sort the resulting map based on the original ordering
        TreeMap<Integer, Tuple> sortedMap = new TreeMap<Integer,Tuple>(selectedTuples);
        
        //TODO: Use a SortedDataBag??
        DataBag outputBag = bagFactory.newDefaultBag();
        
        for (Tuple t : sortedMap.values()) {
            try {
                outputBag.add(tupleFactory.newTuple(t.get(inputSchema.getPosition(this.columnName))));
            } catch (FrontendException fe) {
                throw new ExecException("Unable to find field: " + this.columnName + " in Schema: " + inputSchema.prettyPrint());
            }
            
        }
        
        return outputBag;
    }
    
    @Override
    public byte getDataType() {
        return DataType.BAG;
    }
    
    @Override
    public FieldSchema getSchema(Schema inputSchema) throws FrontendException {
        FieldSchema tupleSchema = new FieldSchema(null, new Schema(inputSchema.getField(this.columnName)), DataType.TUPLE);
        return new FieldSchema(null, new Schema(tupleSchema), this.getDataType());
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
        FuncArgColumn other = (FuncArgColumn) obj;
        
        if (!this.columnName.equals(other.columnName)) {
            return false;
        }
        
        return Arrays.equals(this.symbols, other.symbols);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 61 * hash + (this.columnName != null ? this.columnName.hashCode() : 0);
        hash = 61 * hash + Arrays.deepHashCode(this.symbols);
        return hash;
    }
    
}
