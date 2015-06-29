/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author jasonr
 */
public class FuncArgFactory {
    
    private static final Pattern p = Pattern.compile("(\\w+)\\s+[oO][fF]\\s+(\\w+|\\([\\w\\s\\|]+\\))\\s*");
    
    public static FuncArg createFuncArg(String arg) throws IllegalArgumentException {
        
        if (arg.toLowerCase().equals("null")) {
            return new FuncArgNull();
        }
        else if (arg.startsWith("'") && arg.endsWith("'")) { //String literal
            
            return new FuncArgString(arg.substring(1, arg.length()-1).replace("\'", "'"));
            
        } else if (p.matcher(arg).matches()) { // Column
            
            Matcher m = p.matcher(arg);

            if (m.find()) {
                String columnName         = m.group(1);
                String symbolListUnparsed = m.group(2);
                String[] resultSymbolList = symbolListUnparsed.replaceAll("[\\(\\)\\s]","").split("\\|");
                
                return new FuncArgColumn(columnName, resultSymbolList);
                
            }
            
            throw new IllegalArgumentException("Illegal argument: " + arg + ".");
            
        }
        
        // Else try to parse as an Integer
        return new FuncArgInt(Integer.parseInt(arg));
    }
    
    public static enum FuncArgType {
        INT,
        STRING,
        COLUMN,
        NULL
    };
    
}
