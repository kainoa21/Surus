/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.AccumulatorEvalFunc;

import org.surus.linkanalysis.Result;
import org.surus.linkanalysis.Symbol;
import org.surus.linkanalysis.Tile;
import org.surus.linkanalysis.TileFactory;
import org.surus.linkanalysis.Pattern;
import org.surus.linkanalysis.Pattern.MatchResult;

/**
 *
 * @author jasonr
 */
public class PartitionPatternMatch extends AccumulatorEvalFunc<DataBag> {

    private static final BagFactory bagFactory = BagFactory.getInstance();
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();

    // Intermediate strings needed for building tileList/resultList
    private final String symbolString;
    private final String patternString;
    private final String resultString;
    private final boolean overlapping;
    
    // Store the schema of the input databag which we expect to be in the first index of the input tuple
    private Schema dataBagSchema;
    
    private boolean initialized = false;

    // Final 
    private List<Tile<Tuple>>   tileList   = new ArrayList<Tile<Tuple>>();
    private List<Result> resultList = new ArrayList<Result>();
    Map<String, Symbol>  symbolMap  = new HashMap<String, Symbol>();
    
    // Output Bag - we need to empty this during cleanup
    private DataBag outputBag = bagFactory.newDefaultBag();
    
    // Constructor
    public PartitionPatternMatch(String... inputArgs) {

        if (inputArgs.length < 3 || inputArgs.length > 4)
        	throw new RuntimeException("Invalid parameters list");
    	
        this.symbolString  = inputArgs[0];
        this.patternString = inputArgs[1];
        this.resultString  = inputArgs[2];
        
        if (inputArgs.length == 4) {
            // The a mode was provided, see if it is set to overlapping
            if (inputArgs[3].toLowerCase().equals("overlapping")) {
                this.overlapping = true;
            } else {
                this.overlapping = false;
            }
        } else {
            this.overlapping = false;
        }
        
    }
    
    // Accumulator Interface
    @Override
    public void accumulate(Tuple tuple) throws IOException {
        
        Schema inputSchema = getInputSchema();
    	initialize(inputSchema);

        // We expect the tuple to contain a single Bag
        DataBag bag = (DataBag) tuple.get(0);
        
        List<Pattern<Tuple>> patterns = new ArrayList<Pattern<Tuple>>();
        
        // Loop through the tuples
        for (Tuple t : bag) {
            
            if (overlapping) {
                // create a new pattern object for every input tuple
                patterns.add(new Pattern<Tuple>(this.tileList));
            } else {
                if (patterns.isEmpty()) {
                     patterns.add(new Pattern<Tuple>(this.tileList));
                }
            }
            
            ListIterator<Pattern<Tuple>> iter = patterns.listIterator();
            while (iter.hasNext()) {
                Pattern<Tuple> p = iter.next();
                MatchResult result = p.consume(t);                
                if (result == MatchResult.FAIL) {
                    iter.remove();
                } else if (result == MatchResult.COMPLETE) {
                    outputBag.add(this.aggregate(p.getMatch(), resultList));
                    iter.remove();
                }
            }
            
        }
        
        // Once we are done with the tuples, we need to find out if there are any patterns which are actually matches
        for (Pattern p : patterns) {
            if (p.isMatch()) {
              outputBag.add(this.aggregate(p.getMatch(), resultList));   
            }
        }
        
    }

    @Override
    public void cleanup() {
        this.initialized = false;
    }

    @Override
    public DataBag getValue() {
        return this.outputBag;
    }
    
    @Override
    public Schema outputSchema(Schema input) {

        try {
           initialize(input); 
    
            List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
            for (Result result : resultList) {
                fieldSchemas.add(result.getResultSchema());
            }
            FieldSchema tupleFieldSchema = new FieldSchema(null, new Schema(fieldSchemas), DataType.TUPLE);
            FieldSchema bagFieldSchema = new FieldSchema(this.getClass().getName().toLowerCase(), new Schema(tupleFieldSchema), DataType.BAG);
            return new Schema(bagFieldSchema);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }
    
    public static Map<String, Symbol> parseSymbolString(String inputSymbolString, Schema bagSchema) throws Throwable {

    	Map<String, Symbol> outputSymbolMap = new HashMap<String, Symbol>();
    	
        // Parse Symbols
        String[] symbolList = inputSymbolString.split(";");
        for (String unparsedSymbol : symbolList) {
        	Symbol parsedSymbol = Symbol.parse(unparsedSymbol, bagSchema);
            outputSymbolMap.put(parsedSymbol.getAlias(), parsedSymbol);
        }
        return outputSymbolMap;
    }
    
    public static List<Tile<Tuple>> parsePatternString(String inputPatternString, Map<String, Symbol> inputSymbolMap) {
    	
        
        TileFactory tileFactory = new TileFactory(inputSymbolMap);
        
    	List<Tile<Tuple>> outputTileList = new ArrayList<Tile<Tuple>>();
    	
        // Parse Pattern: replace spaces
    	inputPatternString = inputPatternString.replaceAll("\\s", "");

        // Parse Pattern
        String[] patternList = inputPatternString.split("\\.");
        for (String unparsedPattern : patternList) {

            try {
                Tile tile = tileFactory.createTile(unparsedPattern);
                // Save for later fun
                outputTileList.add(tile);
            } catch (Throwable t) {
                throw new RuntimeException("Error parsing pattern: " + inputPatternString, t);
            }

        }
    	return outputTileList;
    }

    public static List<Result> parseResultString(String inputResultString, Schema bagSchema) throws Throwable {
    
    	List<Result> outputResultList = new ArrayList<Result>();
    	
        // Parse Results Clause
        // Note that this call uses getDataBagSchema so initialize must have already been called
        String[] resultsList = inputResultString.split(";");
        for (String unparsedResults : resultsList) {
            outputResultList.add(Result.parseResult(unparsedResults, bagSchema, UDFContext.getUDFContext().isFrontend()));
        }
        
        return outputResultList;
        
    }
    
    private void initialize(Schema input) throws IOException {
    	
    	if (!initialized) {
            
            this.outputBag.clear();

            // We expect the input schema to be a tuple which has a databag as its first field
            try {
                if (input.size() != 1) {
                    throw new RuntimeException("Expected input to have only a single field");
                }

                Schema.FieldSchema inputFieldSchema = input.getField(0);

                if (inputFieldSchema.type != DataType.BAG) {
                    throw new RuntimeException("Expected a BAG as input");
                }

                Schema inputBagSchema = inputFieldSchema.schema;

                if (inputBagSchema.getField(0).type != DataType.TUPLE) {
                    throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                            DataType.findTypeName(inputBagSchema.getField(0).type)));
                }

                this.dataBagSchema = inputBagSchema.getField(0).schema;

            } catch (FrontendException e) {
                throw new RuntimeException(e);
            }

            try {
                this.symbolMap = parseSymbolString(this.symbolString, this.getDataBagSchema());
            } catch (Throwable t) {
                throw new IOException("Error parsing the symbol string: " + this.symbolString, t);
            }

            this.tileList = parsePatternString(this.patternString, this.symbolMap);

            try {
                this.resultList = parseResultString(this.resultString, this.getDataBagSchema());
            } catch (Throwable t) {
                throw new IOException("Error parsing the result string: " + this.resultString, t);
            }

        }

        initialized = true;

    }
    
    public Schema getDataBagSchema() throws Throwable {
        
        if (this.dataBagSchema == null) {
            throw new Throwable("Call to getDataBagSchema prior to initialization.  You must call initialize first to set this value based on the input schema.");
        }
        return this.dataBagSchema;
    }

    public Tuple aggregate(Map<String, Map<Integer, Tuple>> partitionMatch, List<Result> resultList) throws ExecException {
        
        if (partitionMatch == null || resultList == null || partitionMatch.isEmpty() || resultList.isEmpty()) {
            return null;
        }
        
        Tuple resultTuple = tupleFactory.newTuple();
        
        for (Result result : resultList) {
            
            try {

                // Call the exec method after evaluating arguments
                resultTuple.append(result.execFunc(partitionMatch, getDataBagSchema()));

            } catch (Throwable t) {
                throw new ExecException("Could not invoke exec method on class:" + result.getFuncSpec().getClassName(), t);
            }
        }        
        
        return resultTuple;
        
    }
   
}
