/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

import org.surus.linkanalysis.Pattern.MatchResult;
import org.apache.pig.backend.executionengine.ExecException;

/**
 *
 * @author jasonr
 */
public class ScopedTile<T> extends Tile<T> {
    
    private Integer min = -1;
    private Integer max = -1;
    
    private MatchResult lastMatchResult;
    
    public ScopedTile(Symbol symbol, Integer min, Integer max) {
        super(symbol);
        
        if ((min == null || min < 0) && (max == null || max < 0)) {
            throw new IllegalArgumentException("For a ScopedTile, one of min or max must be an integer greater than or equal to 0.  Current arguments are min=" + min + " and max=" + max);
        }
        
        this.min = min;
        this.max = max;
       
    }
    
    public ScopedTile(ScopedTile<T> tile) {
        this(tile.getSymbol(), tile.min, tile.max);
    }
    
    @Override
    public String toString() {
        if (this.min == null || this.min < 0) {
            return this.getSymbol().toString() + "{," + this.max + "}";
        }
        if (this.max == null || this.max < 0) {
            return this.getSymbol().toString() + "{" + this.min + ",}";
        } 
        return this.getSymbol().toString() + "{" + this.min + "," + this.max + "}";
    }

    @Override
    public boolean isMatch() {
        int matchCount = this.getMatchCount();
        if (this.min == null || this.min < 0) {
            return matchCount <= this.max;
        }
        if (this.max == null || this.max < 0) {
            return matchCount >= this.min;
        } 
        return matchCount >= this.min && matchCount <= this.max;
    }

    @Override
    public MatchResult offer(T t) throws ExecException {        
        this.lastMatchResult = _offer(t);
        return this.lastMatchResult;
    }
    
    public MatchResult _offer(T t) throws ExecException {
        // If we are already at the max, then we fail
        if (this.max != null && this.max >= 0 && this.getMatchCount() == this.max) {
            return MatchResult.FAIL;
        }
        
        // If we don't match then we either are complete or we fail
        if (!this.getSymbol().apply(t)) {
            if (this.isMatch()) {
                return MatchResult.COMPLETE_NO_CONSUME;
            }
            return MatchResult.FAIL;
        }
        
        // If we match, then we are either complete or we continue
        this.addMatch(t);
        if (this.max != null && this.max >= 0 && this.getMatchCount() == this.max) {
            return MatchResult.COMPLETE;
        }
        
        return MatchResult.CONTINUE;
    }

    @Override
    public Tile<T> clone() {
       return new ScopedTile<T>(this);
    }

    @Override
    public MatchResult getLastMatchResult() {
        return this.lastMatchResult;
    }
    
    
}
