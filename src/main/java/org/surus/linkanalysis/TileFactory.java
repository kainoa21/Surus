/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author jasonr
 */
public class TileFactory<T> {
    
    private static final Pattern p = Pattern.compile("^\\s*(\\w+)(\\*|\\+|\\{\\d*,\\d*\\})?\\s*$");
    private static final Pattern scope = Pattern.compile("\\{(\\d*),(\\d*)\\}");
    
    
    private final Map<String, Symbol>  symbolMap;
    
    public TileFactory(Map<String, Symbol>  symbolMap) {
        this.symbolMap = symbolMap;
    }
    
    public Map<String, Symbol> getSymbolMap() {
        return this.symbolMap;
    }
    
    public Tile createTile(String tileString) throws Throwable {
        
        Matcher m = p.matcher(tileString);
        
        if(m.find()) {
            String symbolString = m.group(1);
            String operatorString = m.group(2);
            
            Symbol symbol = symbolMap.get(symbolString);
            
            if (symbol == null) {
                throw new Throwable("Illegal symbol found in pattern: " + symbolString + " is not defined in the symbol list.");
            }
            
            if (operatorString == null || "".equals(operatorString)) { //Base
                return new BaseTile<T>(symbol);
            } else if ("+".equals(operatorString)) {
                return new PlusTile<T>(symbol);
            } else if ("*".equals(operatorString)) {
                return new StarTile<T>(symbol);
            } else if (scope.matcher(operatorString).matches()) {
                Matcher scopeMatcher = scope.matcher(operatorString);
                scopeMatcher.find();
                Integer min = null;
                if (scopeMatcher.group(1) != null && !"".equals(scopeMatcher.group(1))) {
                    min = Integer.parseInt(scopeMatcher.group(1));
                }
                Integer max = null;
                if (scopeMatcher.group(2) != null && !"".equals(scopeMatcher.group(2))) {
                    max = Integer.parseInt(scopeMatcher.group(2));
                }
                return new ScopedTile<T>(symbol, min, max);
            }
        }
        
        throw new Throwable("Unsupported symbol found: " + tileString);
       
    }
    
    public static enum FuncArgType {
        INT,
        STRING,
        COLUMN        
    };
    
}
