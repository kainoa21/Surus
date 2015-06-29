/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 *
 * @author jasonr
 */
public class Symbol<T> {
    
    private final String alias;
    private final List<Predicate> predicateList;
    
    public Symbol(String alias, List<Predicate> predicateList) {
        this.alias = alias;
        this.predicateList = predicateList;
    }

    public String getAlias() {
        return this.alias;
    }

    public List<Predicate> getPredicateList() {
        return this.predicateList;
    }
    
    public boolean apply(T t) throws ExecException {
    	for (Predicate predicate : this.predicateList) {
    		if (!predicate.apply(t)) return false;
    	}
    	return true;
    }
    
    public static Symbol parse(String unparsedSymbol, Schema bagSchema) throws Throwable{

    	String[] splitSymbol 			= unparsedSymbol.split("\\s+[aA][sS]\\s+");
    	String[] unparsedPredicateList 	= splitSymbol[0].split("\\s+[aA][nN][dD]\\s+");
    	String   symbolAlias 			= splitSymbol[1].replaceAll("\\s+", "");

    	
        List<Predicate> predicateList = new ArrayList<Predicate>();
    	for (String unparsedPredicate : unparsedPredicateList)
    		predicateList.add(Predicate.parse(unparsedPredicate, bagSchema) );
    	
        return new Symbol(symbolAlias, predicateList);

    }

	@Override
	public String toString() {
		return "Symbol [alias=" + alias + ", predicateList=" + predicateList + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((alias == null) ? 0 : alias.hashCode());
		result = prime * result
				+ ((predicateList == null) ? 0 : predicateList.hashCode());
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
		Symbol other = (Symbol) obj;
		if (alias == null) {
			if (other.alias != null)
				return false;
		} else if (!alias.equals(other.alias))
			return false;
		if (predicateList == null) {
			if (other.predicateList != null)
				return false;
		} else if (!predicateList.equals(other.predicateList))
			return false;
		return true;
	}


    
}
