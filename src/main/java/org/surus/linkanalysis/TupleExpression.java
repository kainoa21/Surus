/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;


/**
 *
 * @author jasonr
 */
public class TupleExpression extends Expression<Tuple> {
    
    public TupleExpression(String value) {
        super(value);
    }
    
    public TupleExpression(String value, int columnIndex) {
        super(value,columnIndex);
    }
   
    @Override
    public Object Evaluate(Tuple t) throws ExecException {
        if (this.getColumnIndex() >= 0) {
            return t.get(this.getColumnIndex());
        }
        return this.getValue();    
    }


}
