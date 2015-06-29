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
public class BaseTile<T> extends Tile<T> {
    
    private boolean isMatched = false;
    private MatchResult lastMatchResult;
    
    public BaseTile(Symbol symbol) {
        super(symbol);
    }
    
    public BaseTile(Tile<T> tile) {
        super(tile);
    }
    
    @Override
    public boolean isMatch() {
        return isMatched;
    }

    @Override
    public MatchResult offer(T t) throws ExecException {
        
        this.lastMatchResult = _offer(t);
        return this.lastMatchResult;
              
    }
    
    public MatchResult _offer(T t) throws ExecException {
        if (this.getSymbol().apply(t)) {
            this.isMatched = true;
            this.addMatch(t);
            return MatchResult.COMPLETE;
        }
        
        return MatchResult.FAIL;  
    }

    @Override
    public Tile<T> clone() {
        return new BaseTile<T>(this);
    }
    
    @Override
    public MatchResult getLastMatchResult() {
        return this.lastMatchResult;
    }

    
}
