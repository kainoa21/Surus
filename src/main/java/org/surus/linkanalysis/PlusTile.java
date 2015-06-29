/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

/**
 *
 * @author jasonr
 */
public class PlusTile<T> extends ScopedTile<T> {
    
    public PlusTile(Symbol symbol) {
        super(symbol, 1, null);
    }

    @Override
    public String toString() {
        return this.getSymbol().toString() + "+";
    }
    
}
