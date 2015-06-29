/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

/**
 *
 * @author jasonr
 */
public class QuestionTile<T> extends ScopedTile<T> {
    
    public QuestionTile(Symbol symbol) {
        super(symbol, 0, 1);
    }

    @Override
    public String toString() {
        return this.getSymbol().toString() + "?";
    }
    
}
