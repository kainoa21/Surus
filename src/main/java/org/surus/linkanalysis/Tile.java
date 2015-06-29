/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

import org.surus.linkanalysis.Pattern.MatchResult;
import java.util.ArrayList;
import java.util.List;
import org.apache.pig.backend.executionengine.ExecException;

/**
 *
 * @author jasonr
 */
public abstract class Tile<T> implements Cloneable {
    
    private final Symbol<T> symbol;
    private final List<T> matches = new ArrayList<T>();

    public Tile(Symbol<T> symbol) {
        this.symbol = symbol;
    }
    
    public Tile(Tile<T> tile) {
        this.symbol = tile.getSymbol();
    }

    public Symbol<T> getSymbol() {
        return this.symbol;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
        return result;
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
        Tile other = (Tile) obj;
        if (symbol == null) {
            if (other.symbol != null) {
                return false;
            }
        } else if (!symbol.equals(other.symbol)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
    	return symbol.toString();
    }
    
    public void addMatch(T t) {
        this.matches.add(t);
    }
    
    public List<T> getMatches() {
        return this.matches;
    }
    
    public int getMatchCount() {
        return this.matches.size();
    }
    
    @Override
    public abstract Tile<T> clone();
    
    public abstract MatchResult offer(T t) throws ExecException;
    
    public abstract boolean isMatch();
    
    public abstract MatchResult getLastMatchResult();
    
    //public abstract TileMatch match(PeekingIterator<Tuple> inputTuples) throws ExecException;
   
}
