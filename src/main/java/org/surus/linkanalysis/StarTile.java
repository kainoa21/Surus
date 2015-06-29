/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

/**
 *
 * @author jasonr
 */
public class StarTile<T> extends ScopedTile<T> {
    
    public StarTile(Symbol symbol) {
        super(symbol, 0, null);
    }

    @Override
    public String toString() {
        return this.getSymbol().toString() + "*";
    }
    
}
