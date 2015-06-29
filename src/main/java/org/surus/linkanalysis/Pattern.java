/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.apache.pig.backend.executionengine.ExecException;

/**
 *
 * @author jasonr
 */
public class Pattern<T> {
    
    private final List<Tile<T>> tiles;
    private final ListIterator<Tile<T>> iter;
    private Tile<T> curTile;
    
    private Map<String,Map<Integer, T>> matches = new HashMap<String,Map<Integer, T>>();
    
    private int matchIndex = 1;
    
    public Pattern(List<Tile<T>> tiles) {
        this.tiles = tiles;
        iter = tiles.listIterator();
    }
    
    public List<Tile<T>> getTiles() {
        return this.tiles;
    }
    
    public Map<String,Map<Integer, T>> getMatch() {
        // Add the matches from the current tile if the last match result was continue and it is a match
        if (getCurrentTile().getLastMatchResult() == MatchResult.CONTINUE && getCurrentTile().isMatch()) {
            this.addMatches(getCurrentTile().getSymbol().getAlias(), getCurrentTile().getMatches());
        }
        return this.matches;
    }
    
    public boolean isMatch() {
        return !iter.hasNext() && getCurrentTile().isMatch();
    }
    
    public MatchResult consume(T obj) throws ExecException {
        
        Tile<T> tile = getCurrentTile();
        
        if (tile == null) {
            return MatchResult.COMPLETE;
        }
        
        MatchResult result = tile.offer(obj);
        
        while(result == MatchResult.COMPLETE_NO_CONSUME) {
            
            // Grab the matches from this tile
            this.addMatches(tile.getSymbol().getAlias(), tile.getMatches());
            
            // Try and move to the next tile
            if (!getNextTile()) {
                // We are done so we can return complete
                return MatchResult.COMPLETE;
            }
            
            // Offer this input again until we either finish all the tiles or consume this input
            tile = getCurrentTile();
            result = tile.offer(obj);
        }
        
        // If the tile failes then we fail
        if (result == MatchResult.FAIL) {
            return MatchResult.FAIL;                
        }
        
        if (result == MatchResult.COMPLETE) {
            
            // Grab the matches from this tile
            this.addMatches(tile.getSymbol().getAlias(), tile.getMatches());
            
            // Check to see if we have completed all the tiles
            if (!getNextTile()) {
                // We are done so we can return complete
                return MatchResult.COMPLETE;
            }
        }
        
        return MatchResult.CONTINUE;
        
    }

    private void addMatches(String alias, List<T> matchList) {
        
        // Grab the matches from this tile
        if (this.matches.containsKey(alias)) {
            for (T match : matchList) {
                matches.get(alias).put(matchIndex, match);
                matchIndex++;
            }
        } else {
            Map<Integer, T> tileMatches = new HashMap<Integer, T>();
            for (T match : matchList) {
                tileMatches.put(matchIndex, match);
                matchIndex++;
            }
            matches.put(alias, tileMatches);
        }
    }
    
    protected Tile<T> getCurrentTile() {
       if (curTile == null) {
          curTile =  iter.next().clone();
       }
       return curTile;
    }
    
    protected boolean getNextTile() {
        if (iter.hasNext()) {
            this.curTile = iter.next().clone();
            return true;
        }
        return false;
    }
            
    public static enum MatchResult {
        FAIL,
        CONTINUE,
        COMPLETE,
        COMPLETE_NO_CONSUME
    };
     
}
