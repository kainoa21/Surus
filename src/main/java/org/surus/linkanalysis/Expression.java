/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

import org.apache.pig.backend.executionengine.ExecException;

/**
 *
 * @author jasonr
 */
public abstract class Expression<T> {
    
    private final String value;
    private final int columnIndex;
    
    public Expression(String value) {
        this.value = value;
        this.columnIndex = -1;
    }
    
    public Expression(String value, int columnIndex) {
        this.value = value;
        this.columnIndex = columnIndex;
    }
    
    public abstract Object Evaluate(T t) throws ExecException;
    
    public String getValue() {return this.value;}
    public int getColumnIndex() {return this.columnIndex;}
    
    public String toString(){
    	if (columnIndex > 0) 
    		return this.value;
    	else
    		return this.value;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + columnIndex;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Expression other = (Expression) obj;
		if (columnIndex != other.columnIndex)
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

}
