package org.surus.linkanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.newplan.logical.Util;

public class Result {

    private final static TupleFactory tf = TupleFactory.getInstance();
    private static final int INF = -1;
    
    // Eric's awesome regex match
    // Example: "package.CLASS( column of (A|B) , 42, 'arg3' ) as outputAlias"
    private static final Pattern p = Pattern.compile("([\\w\\.]+)\\(([\\s\\w\\d\\'|,\\(\\)]*)\\)\\s+[aA][sS]\\s+(\\w+)");
    
    private final String outputAlias;
    private final FuncSpec funcSpec;
    private final List<FuncArg> argList;
    private final Schema inputArgsSchema;
    private EvalFunc<?> ef = null;
    private boolean isViaDefine = false;
    
    public Result(String funcSpec, List<FuncArg> args, Schema inputArgsSchema, String outputAlias, boolean isFrontend) {
        
        FuncSpec bestFitFuncSpec = null;
        if (!isFrontend) {
            
            try {
                // Then we can try using the pig context to get the funcSpec spec from the alias list
                Configuration jobConf = UDFContext.getUDFContext().getJobConf();
                PigContext pigContext = (PigContext) ObjectSerializer.deserialize(jobConf.get("pig.pigContext"));

                // See if this function has already been defined by the user
                bestFitFuncSpec = pigContext.getFuncSpecFromAlias(funcSpec);
                
                // If we didn't find it, try using just the Class Name w/o the class path
                if (bestFitFuncSpec == null && funcSpec.contains(".")) {
                    //System.out.println("ALIAS: Not found for " + funcSpec);
                    String[] classPathParts = funcSpec.split("\\.");
                    if (classPathParts.length > 0) {
                        //System.out.println("ALIAS: Trying " + classPathParts[classPathParts.length - 1]);
                        bestFitFuncSpec = pigContext.getFuncSpecFromAlias(classPathParts[classPathParts.length - 1]);
                    }                    
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

        }
        
        
        if (bestFitFuncSpec != null) {
            this.isViaDefine = true;
            bestFitFuncSpec.setInputArgsSchema(inputArgsSchema);
        } else {
            //System.out.println("ALIAS: No define: for " + funcSpec);
            bestFitFuncSpec = new FuncSpec(funcSpec, inputArgsSchema);
        }
        
        try {
            bestFitFuncSpec = this.findBestFitFuncSpec(bestFitFuncSpec);
        } catch (Throwable t) {
            t.printStackTrace();
        }
        
        
        this.funcSpec = bestFitFuncSpec;
        this.inputArgsSchema = inputArgsSchema;
        this.argList = args;
        this.outputAlias = outputAlias;
        
        
        
    }

    public String getOutputAlias() {
        return this.outputAlias;
    }

    public FuncSpec getFuncSpec() {
        
        return this.funcSpec;
    }

    public List<FuncArg> getFuncArgs() {
        return this.argList;
    }

    public EvalFunc<?> getEvalFunc() {
        if (ef == null) {
            ef = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(this.funcSpec);
            
            // If the input schema isn't set then we will try and set it
            if (ef.getInputSchema() == null) {
                if (this.funcSpec.getInputArgsSchema() != null) { // If we have the schema from the funcSpec then use that
                    ef.setInputSchema(this.funcSpec.getInputArgsSchema());
                } else { // Use the schema that was passed to us (this *may* not exaclty match the schema of the best fit EvalFunc?
                    ef.setInputSchema(inputArgsSchema);
                }
            }
            
            
        }
        return this.ef;
    }
    
    public boolean isViaDefine() {
        return this.isViaDefine;
    }

    public Object execFunc(Map<String, Map<Integer, Tuple>> matchGroup, Schema inputSchema) throws IOException {

        // Evaluate the arguments
        List<Object> argValues = new ArrayList<Object>();
        for (FuncArg arg : this.argList) {
            //TODO: We may need to cast these in order for the types to line up with the
            //      actual function we are calling based on the funcSpec
            argValues.add(arg.getValue(matchGroup, inputSchema));
        }

        // Call exec on the funcSpec
        return this.getEvalFunc().exec(tf.newTupleNoCopy(argValues));

    }
    
    public static Result parseResult(String result, Schema inputSchema, boolean isFrontend) throws Throwable {
        
        Matcher m = p.matcher(result);

        if (m.find()) {
            String funcName = m.group(1);
            String args = m.group(2);
            String outputAlias = m.group(3);

            List<FuncArg> funcArgs = new ArrayList<FuncArg>();
            List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();

            String[] argStrings = args.split(",");
            for (String arg : argStrings) {
                FuncArg funcArg = FuncArgFactory.createFuncArg(arg.trim());
                funcArgs.add(FuncArgFactory.createFuncArg(arg.trim()));
                fieldSchemas.add(funcArg.getSchema(inputSchema));
            }

            return new Result(funcName, funcArgs, new Schema(fieldSchemas), outputAlias, isFrontend);

            // Check to make sure we can load this function (thanks Cheolsoo!!)
//                Configuration jobConf = UDFContext.getUDFContext().getJobConf();
//                PigContext pigContext = (PigContext)ObjectSerializer.deserialize(jobConf.get("pig.pigContext"));
//                
//                // See if this function has already been defined by the user
//                FuncSpec fSpec = pigContext.getFuncSpecFromAlias(funcName);
//                
//                // Otherwise, try and build one
//                if(fSpec == null) {
//                    fSpec = new FuncSpec(funcName);
//                }
//                
//                EvalFunc funcSpec = (EvalFunc) PigContext.instantiateFuncFromSpec(fSpec);
//                

            // TODO: Check that alias is in symbol map

        }
        
        throw new Throwable("Result string could not be parsed: " + result + ".  Expected format is package.CLASS( column of (A|B) , 42, 'arg3' ) as outputAlias");
    }
    
    public FieldSchema getResultSchema() throws FrontendException {
        // We need to wrap our inputArgsSchema inside of a Tuple before passing to the outputSchema function
        //System.out.println(funcSpec + ": " + this.funcSpec.getInputArgsSchema().prettyPrint());
        //System.out.println("inputArgsSchema: " + this.inputArgsSchema.prettyPrint());
        //System.out.println("EvalFunc: " + this.getEvalFunc().getInputSchema().prettyPrint());
        Schema evalFuncOutputSchema = this.getEvalFunc().outputSchema(this.funcSpec.getInputArgsSchema());        
        //System.out.println(evalFuncOutputSchema.toString());
        FieldSchema returnSchema = evalFuncOutputSchema.getField(0);
        returnSchema.alias = this.getOutputAlias();
        return returnSchema;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(this.funcSpec.getClassName());
        sb.append("(");
        for (FuncArg arg : this.argList) {
            sb.append(arg.toString());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(") as ");
        sb.append(this.outputAlias);
        return sb.toString();
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
        Result other = (Result) obj;
        if (funcSpec == null) {
            if (other.funcSpec != null) {
                return false;
            }
        } else if (!funcSpec.equals(other.funcSpec)) {
            return false;
        }
        if (inputArgsSchema == null) {
            if (other.inputArgsSchema != null) {
                return false;
            }
        } else if (!inputArgsSchema.equals(other.inputArgsSchema)) {
            return false;
        }
        if (outputAlias == null) {
            if (other.outputAlias != null) {
                return false;
            }
        } else if (!outputAlias.equals(other.outputAlias)) {
            return false;
        }
        if (argList == null) {
            if (other.argList != null) {
                return false;
            }
        } else if (!argList.equals(other.argList)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 19 * hash + (this.outputAlias != null ? this.outputAlias.hashCode() : 0);
        hash = 19 * hash + (this.funcSpec != null ? this.funcSpec.hashCode() : 0);
        hash = 19 * hash + (this.argList != null ? this.argList.hashCode() : 0);
        hash = 19 * hash + (this.inputArgsSchema != null ? this.inputArgsSchema.hashCode() : 0);
        return hash;
    }
    
    
    private FuncSpec findBestFitFuncSpec(FuncSpec fSpec) throws Throwable {
        
        /**
         * Here is an explanation of the way the matching UDF funcspec will be
         * chosen based on actual types in the input schema. First an "exact"
         * match is tried for each of the fields in the input schema with the
         * corresponding fields in the candidate funcspecs' schemas.
         *
         * If exact match fails, then first a check if made if the input schema
         * has any bytearrays in it.
         *
         * If there are NO bytearrays in the input schema, then a best fit match
         * is attempted for the different fields. Essential a permissible cast
         * from one type to another is given a "score" based on its position in
         * the "castLookup" table. A final score for a candidate funcspec is
         * deduced as SUM(score_of_particular_cast*noOfCastsSoFar). If no
         * permissible casts are possible, the score for the candidate is -1.
         * Among the non -1 score candidates, the candidate with the lowest
         * score is chosen.
         *
         * If there are bytearrays in the input schema, a modified exact match
         * is tried. In this matching, bytearrays in the input schema are not
         * considered. As a result of ignoring the bytearrays, we could get
         * multiple candidate funcspecs which match "exactly" for the other
         * columns - if this is the case, we notify the user of the ambiguity
         * and error out. Else if all other (non byte array) fields matched
         * exactly, then we can cast bytearray(s) to the corresponding type(s)
         * in the matched udf schema. If this modified exact match fails, the
         * above best fit algorithm is attempted by initially coming up with
         * scores and candidate funcSpecs (with bytearray(s) being ignored in
         * the scoring process). Then a check is made to ensure that the
         * positions which have bytearrays in the input schema have the same
         * type (for a given position) in the corresponding positions in all the
         * candidate funcSpecs. If this is not the case, it indicates a conflict
         * and the user is notified of the error (because we have more than one
         * choice for the destination type of the cast for the bytearray). If
         * this is the case, the candidate with the lowest score is chosen.
         */
        
        FuncSpec matchingSpec = null;
        Schema currentArgSchema = fSpec.getInputArgsSchema();
        try {
            // Check to see if there is a mapping to a differnt class based on the input arg types
            EvalFunc<?> func = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(fSpec);
            List<FuncSpec> funcSpecs = func.getArgToFuncMapping();

            boolean notExactMatch = false;
            if (funcSpecs != null && !funcSpecs.isEmpty()) {
                //Some function mappings found. Trying to see
                //if one of them fits the input schema
                if ((matchingSpec = exactMatch(funcSpecs, currentArgSchema, fSpec)) == null) {
                    //Oops, no exact match found. Trying to see if we
                    //have mappings that we can fit using casts.
                    notExactMatch = true;
                    if (byteArrayFound(currentArgSchema)) {
                        // try "exact" matching all other fields except the byte array
                        // fields and if they all exact match and we have only one candidate
                        // for the byte array cast then that's the matching one!
                        if ((matchingSpec = exactMatchWithByteArrays(funcSpecs, currentArgSchema, fSpec)) == null) {
                            // "exact" match with byte arrays did not work - try best fit match
                            if ((matchingSpec = bestFitMatchWithByteArrays(funcSpecs, currentArgSchema, fSpec)) == null) {
                                int errCode = 1045;
                                String msg = "Could not infer the matching function for "
                                        + fSpec
                                        + " as multiple or none of them fit. Please use an explicit cast.";
                                throw new FrontendException(msg);
                            }
                        }
                    } else if ((matchingSpec = bestFitMatch(funcSpecs, currentArgSchema)) == null) {
                        // Either no byte arrays found or there are byte arrays
                        // but only one mapping exists.
                        // However, we could not find a match as there were either
                        // none fitting the input schema or it was ambiguous.
                        // Throw exception that we can't infer a fit.
                        int errCode = 1045;
                        String msg = "Could not infer the matching function for "
                                + fSpec
                                + " as multiple or none of them fit. Please use an explicit cast.";
                        //msgCollector.collect(msg, MessageType.Error);
                        throw new FrontendException(msg);
                    }
                }
            }
            if (matchingSpec != null) {
                //Voila! We have a fitting match. Lets insert casts and make
                //it work.
                // notify the user about the match we picked if it was not
                // an exact match
                if (notExactMatch) {
                    String msg = "Function " + fSpec.getClassName() + "()"
                            + " will be called with following argument types: "
                            + matchingSpec.getInputArgsSchema() + ". If you want to use "
                            + "different input argument types, please use explicit casts.";
                    //msgCollector.collect(msg, MessageType.Warning, PigWarning.USING_OVERLOADED_FUNCTION);
                }
                if (this.isViaDefine()) {
                    matchingSpec.setCtorArgs(fSpec.getCtorArgs());
                }
                return matchingSpec;
                //insertCastsForUDF(funcSpec, currentArgSchema, matchingSpec.getInputArgsSchema());

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        // We did not find a better match
        return fSpec;
        
    }
    
    /**
     * Tries to find the schema supported by one of funcSpecs which can be
     * obtained by inserting a set of casts to the input schema
     *
     * @param funcSpecs -
     *            mappings provided by udf
     * @param s -
     *            input schema
     * @param funcSpec -
     *             udf expression
     * @param udfSchemaType - 
     *            schema type of the udf
     * @return the funcSpec that supports the schema that is best suited to s.
     *         The best suited schema is one that has the lowest score as
     *         returned by fitPossible().
     * @throws VisitorException
     */
    private FuncSpec bestFitMatchWithByteArrays(List<FuncSpec> funcSpecs, Schema s, FuncSpec origFuncSpec) throws Throwable {
        List<Pair<Long, FuncSpec>> scoreFuncSpecList = new ArrayList<Pair<Long, FuncSpec>>();
        for (Iterator<FuncSpec> iterator = funcSpecs.iterator(); iterator
                .hasNext();) {
            FuncSpec fs = iterator.next();
            long score = fitPossible(s, fs.getInputArgsSchema());
            if (score != INF) {
                scoreFuncSpecList.add(new Pair<Long, FuncSpec>(score, fs));
            }
        }

        // if no candidates found, return null
        if(scoreFuncSpecList.isEmpty())
            return null;

        if(scoreFuncSpecList.size() > 1) {
            // sort the candidates based on score
            Collections.sort(scoreFuncSpecList, new ScoreFuncSpecListComparator());

            // if there are two (or more) candidates with the same *lowest* score
            // we cannot choose one of them - notify the user
            if (scoreFuncSpecList.get(0).first == scoreFuncSpecList.get(1).first) {
                int errCode = 1046;
                String msg = "Multiple matching functions for "
                        + origFuncSpec + " with input schemas: " + "("
                        + scoreFuncSpecList.get(0).second.getInputArgsSchema() + ", "
                        + scoreFuncSpecList.get(1).second.getInputArgsSchema() + "). Please use an explicit cast.";
                //msgCollector.collect(msg, MessageType.Error);
                throw new Throwable(msg);
            }

            // now consider the bytearray fields
            List<Integer> byteArrayPositions = getByteArrayPositions(s);
            // make sure there is only one type to "cast to" for the byte array
            // positions among the candidate funcSpecs
            Map<Integer, Pair<FuncSpec, Byte>> castToMap = new HashMap<Integer, Pair<FuncSpec, Byte>>();
            for (Iterator<Pair<Long, FuncSpec>> it = scoreFuncSpecList.iterator(); it.hasNext();) {
                FuncSpec funcSpec = it.next().second;
                Schema sch = funcSpec.getInputArgsSchema();
                for (Iterator<Integer> iter = byteArrayPositions.iterator(); iter
                        .hasNext();) {
                    Integer i = iter.next();
                    try {
                        if (!castToMap.containsKey(i)) {
                            // first candidate
                            castToMap.put(i, new Pair<FuncSpec, Byte>(funcSpec, sch
                                    .getField(i).type));
                        } else {
                            // make sure the existing type from an earlier candidate
                            // matches
                            Pair<FuncSpec, Byte> existingPair = castToMap.get(i);
                            if (sch.getField(i).type != existingPair.second) {
                                int errCode = 1046;
                                String msg = "Multiple matching functions for "
                                        + origFuncSpec + " with input schema: "
                                        + "(" + existingPair.first.getInputArgsSchema()
                                        + ", " + funcSpec.getInputArgsSchema()
                                        + "). Please use an explicit cast.";
                                //msgCollector.collect(msg, MessageType.Error);
                                throw new Throwable(msg);
                            }
                        }
                    } catch (FrontendException fee) {
                        int errCode = 1043;
                        String msg = "Unalbe to retrieve field schema.";
                        throw new Throwable(msg, fee);
                    }
                }
            }
        }

        // if we reached here, it means we have >= 1 candidates and these candidates
        // have the same type for position which have bytearray in the input
        // Also the candidates are stored sorted by score in a list - we can now
        // just return the first candidate (the one with the lowest score)
        return scoreFuncSpecList.get(0).second;
    }


    private static class ScoreFuncSpecListComparator implements Comparator<Pair<Long, FuncSpec>> {

        /* (non-Javadoc)
         * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
         */
        public int compare(Pair<Long, FuncSpec> o1, Pair<Long, FuncSpec> o2) {
            if(o1.first < o2.first)
                return -1;
            else if (o1.first > o2.first)
                return 1;
            else
                return 0;
        }

    }

    /**
     * Finds if there is an exact match between the schema supported by
     * one of the funcSpecs and the input schema s. Here first exact match
     * for all non byte array fields is first attempted and if there is
     * exactly one candidate, it is chosen (since the bytearray(s) can
     * just be cast to corresponding type(s) in the candidate)
     * @param funcSpecs - mappings provided by udf
     * @param s - input schema
     * @param funcSpec - UserFuncExpression for which matching is requested
     * @param udfSchemaType - schema type of the udf
     * @return the matching spec if found else null
     * @throws FrontendException
     */
    private FuncSpec exactMatchWithByteArrays(List<FuncSpec> funcSpecs, Schema s, FuncSpec origFuncSpec) throws Throwable {
        // exact match all fields except byte array fields
        // ignore byte array fields for matching
        return exactMatchHelper(funcSpecs, s, origFuncSpec, true);
    }

    /**
     * Finds if there is an exact match between the schema supported by
     * one of the funcSpecs and the input schema s. Here an exact match
     * for all fields is attempted.
     * @param funcSpecs - mappings provided by udf
     * @param s - input schema
     * @param funcSpec - UserFuncExpression for which matching is requested
     * @param udfSchemaType - schema type of the user defined function
     * @return the matching spec if found else null
     * @throws FrontendException
     */
    private FuncSpec exactMatch(List<FuncSpec> funcSpecs, Schema s, FuncSpec origFuncSpec) throws Throwable {
        // exact match all fields, don't ignore byte array fields
        return exactMatchHelper(funcSpecs, s, origFuncSpec, false);
    }

    /**
     * Tries to find the schema supported by one of funcSpecs which can
     * be obtained by inserting a set of casts to the input schema
     * @param funcSpecs - mappings provided by udf
     * @param s - input schema
     * @param udfSchemaType - schema type of the udf
     * @return the funcSpec that supports the schema that is best suited
     *          to s. The best suited schema is one that has the
     *          lowest score as returned by fitPossible().
     */
    private FuncSpec bestFitMatch(List<FuncSpec> funcSpecs, Schema s) {
        FuncSpec matchingSpec = null;
        long score = INF;
        long prevBestScore = Long.MAX_VALUE;
        long bestScore = Long.MAX_VALUE;
        for (Iterator<FuncSpec> iterator = funcSpecs.iterator(); iterator.hasNext();) {
            FuncSpec fs = iterator.next();
            score = fitPossible(s,fs.getInputArgsSchema());
            if(score!=INF && score<=bestScore){
                matchingSpec = fs;
                prevBestScore = bestScore;
                bestScore = score;
            }
        }
        if(matchingSpec!=null && bestScore!=prevBestScore)
            return matchingSpec;

        return null;
    }

    /**
     * Checks to see if any field of the input schema is a byte array
     * @param funcSpec
     * @param s - input schema
     * @return true if found else false
     * @throws VisitorException
     */
    private boolean byteArrayFound(Schema s) throws Throwable {
        for(int i=0;i<s.size();i++){
            try {
                FieldSchema fs=s.getField(i);
                if(fs == null)
                    return false;
                if(fs.type==DataType.BYTEARRAY){
                    return true;
                }
            } catch (FrontendException fee) {
                int errCode = 1043;
                String msg = "Unable to retrieve field schema.";
                throw new Throwable(msg, fee);
            }
        }
        return false;
    }

    /**
     * Gets the positions in the schema which are byte arrays
     * @param funcSpec
     *
     * @param s -
     *            input schema
     * @throws VisitorException
     */
    private List<Integer> getByteArrayPositions(Schema s) throws Throwable {
        List<Integer> result = new ArrayList<Integer>();
        for (int i = 0; i < s.size(); i++) {
            try {
                FieldSchema fs = s.getField(i);
                if (fs.type == DataType.BYTEARRAY) {
                    result.add(i);
                }
            } catch (FrontendException fee) {
                int errCode = 1043;
                String msg = "Unable to retrieve field schema.";
                throw new Throwable(msg, fee);            }
        }
        return result;
    }

    /**
     * Finds if there is an exact match between the schema supported by
     * one of the funcSpecs and the input schema s
     * @param funcSpecs - mappings provided by udf
     * @param s - input schema
     * @param funcSpec user defined function
     * @param udfSchemaType - schema type of the user defined function
     * @param ignoreByteArrays - flag for whether the exact match is to computed
     * after ignoring bytearray (if true) or without ignoring bytearray (if false)
     * @return the matching spec if found else null
     * @throws FrontendException
     */
    private FuncSpec exactMatchHelper(List<FuncSpec> funcSpecs, Schema s, FuncSpec origFuncSpec, boolean ignoreByteArrays) throws Throwable {
        List<FuncSpec> matchingSpecs = new ArrayList<FuncSpec>();
        for (Iterator<FuncSpec> iterator = funcSpecs.iterator(); iterator.hasNext();) {
            FuncSpec fs = iterator.next();
            if (schemaEqualsForMatching(s, fs.getInputArgsSchema(), ignoreByteArrays)) {
                matchingSpecs.add(fs);

            }
        }
        if(matchingSpecs.isEmpty())
            return null;

        if(matchingSpecs.size() > 1) {
            int errCode = 1046;
            String msg = "Multiple matching functions for "
                                        + origFuncSpec + " with input schema: "
                                        + "(" + matchingSpecs.get(0).getInputArgsSchema()
                                        + ", " + matchingSpecs.get(1).getInputArgsSchema()
                                        + "). Please use an explicit cast.";
            //msgCollector.collect(msg, MessageType.Error);
            throw new Throwable(msg);
        }

        // exactly one matching spec - return it
        return matchingSpecs.get(0);
    }
    
     /***************************************************************************
     * Compare two schemas for equality for argument matching purposes. This is
     * a more relaxed form of Schema.equals wherein first the Datatypes of the
     * field schema are checked for equality. Then if a field schema in the udf
     * schema is for a complex type AND if the inner schema is NOT null, check
     * for schema equality of the inner schemas of the UDF field schema and
     * input field schema
     *
     * @param inputSchema
     * @param udfSchema
     * @param ignoreByteArrays
     * @return true if FieldSchemas are equal for argument matching, false
     *         otherwise
     * @throws FrontendException
     */
    public static boolean schemaEqualsForMatching(Schema inputSchema, Schema udfSchema, boolean ignoreByteArrays) throws FrontendException {


        // If both of them are null, they are equal
        if ((inputSchema == null) && (udfSchema == null)) {
            return true;
        }

        // otherwise
        if (inputSchema == null) {
            return false;
        }

        if (udfSchema == null) {
            return false;
        }

        // the old udf schemas might not have tuple inside bag
        // fix that!
        udfSchema = Util.fixSchemaAddTupleInBag(udfSchema);

        if ((inputSchema.size() != udfSchema.size()))
            return false;

        Iterator<FieldSchema> i = inputSchema.getFields().iterator();
        Iterator<FieldSchema> j = udfSchema.getFields().iterator();

        FieldSchema udfFieldSchema = null;
        while (i.hasNext()) {

            FieldSchema inputFieldSchema = i.next();
            if(inputFieldSchema == null)
                return false;

            //if there's no more UDF field: take the last one which is the vararg field
            udfFieldSchema = j.hasNext() ? j.next() : udfFieldSchema;
            
            if(ignoreByteArrays && inputFieldSchema.type == DataType.BYTEARRAY) {
                continue;
            }
            
            if (inputFieldSchema.type != udfFieldSchema.type) {
                return false;
            }

            // if a field schema in the udf schema is for a complex
            // type AND if the inner schema is NOT null, check for schema
            // equality of the inner schemas of the UDF field schema and
            // input field schema. If the field schema in the udf schema is
            // for a complex type AND if the inner schema IS null it means
            // the udf is applicable for all input which has the same type
            // for that field (irrespective of inner schema)
            // if it is a bag with empty tuple, then just rely on the field type
            if (DataType.isSchemaType(udfFieldSchema.type)
                    && udfFieldSchema.schema != null
                    && isNotBagWithEmptyTuple(udfFieldSchema)
            ) {
                // Compare recursively using field schema
                if (!FieldSchema.equals(inputFieldSchema, udfFieldSchema,
                        false, true)) {
                    //try modifying any empty tuple to type of bytearray
                    // and see if that matches. Need to do this for
                    // backward compatibility -
                    // User might have specified tuple with a bytearray
                    // and this should also match an empty tuple

                    FieldSchema inputFSWithBytearrayinTuple =
                        new FieldSchema(inputFieldSchema);

                    convertEmptyTupleToBytearrayTuple(inputFSWithBytearrayinTuple);

                    if (!FieldSchema.equals(inputFSWithBytearrayinTuple, udfFieldSchema,
                            false, true)) {
                        return false;
                    }
                }
            }

        }
        return true;
    }
    
    /**
     * Check if the fieldSch is a bag with empty tuple schema
     * @param fieldSch
     * @return
     * @throws FrontendException
     */
    private static boolean isNotBagWithEmptyTuple(FieldSchema fieldSch)
    throws FrontendException {
        boolean isBagWithEmptyTuple = false;
        if(fieldSch.type == DataType.BAG
                && fieldSch.schema != null
                && fieldSch.schema.getField(0) != null
                && fieldSch.schema.getField(0).type == DataType.TUPLE
                && fieldSch.schema.getField(0).schema == null
        ){
            isBagWithEmptyTuple = true;
        }
        return !isBagWithEmptyTuple;
    }

    private static void convertEmptyTupleToBytearrayTuple(
            FieldSchema fs) {
        if(fs.type == DataType.TUPLE
                && fs.schema != null
                && fs.schema.size() == 0){
            fs.schema.add(new FieldSchema(null, DataType.BYTEARRAY));
            return;
        }

        if(fs.schema != null){
            for(FieldSchema inFs : fs.schema.getFields()){
                convertEmptyTupleToBytearrayTuple(inFs);
            }
        }

    }

    static final HashMap<Byte, List<Byte>> castLookup = new HashMap<Byte, List<Byte>>();
    static{
        //Ordering here decides the score for the best fit function.
        //Do not change the order. Conversions to a smaller type is preferred
        //over conversion to a bigger type where ordering of types is:
        //INTEGER, LONG, FLOAT, DOUBLE, DATETIME, CHARARRAY, TUPLE, BAG, MAP
        //from small to big

        List<Byte> boolToTypes = Arrays.asList(
                DataType.INTEGER,
                DataType.LONG,
                DataType.FLOAT,
                DataType.DOUBLE
                //DataType.BIGINTEGER,
                //DataType.BIGDECIMAL
                // maybe more bigger types
        );
        castLookup.put(DataType.BOOLEAN, boolToTypes);

        List<Byte> intToTypes = Arrays.asList(
                DataType.LONG,
                DataType.FLOAT,
                DataType.DOUBLE
                //DataType.BIGINTEGER,
                //DataType.BIGDECIMAL
        );
        castLookup.put(DataType.INTEGER, intToTypes);

        List<Byte> longToTypes = Arrays.asList(
                DataType.FLOAT,
                DataType.DOUBLE
                //DataType.BIGINTEGER,
                //DataType.BIGDECIMAL
        );
        castLookup.put(DataType.LONG, longToTypes);

        List<Byte> floatToTypes = Arrays.asList(
                DataType.DOUBLE
                //DataType.BIGINTEGER,
                //DataType.BIGDECIMAL
        );
        castLookup.put(DataType.FLOAT, floatToTypes);

        List<Byte> doubleToTypes = Arrays.asList(
                //DataType.BIGINTEGER,
                //DataType.BIGDECIMAL
        );
        castLookup.put(DataType.DOUBLE, doubleToTypes);

        List<Byte> bigIntegerToTypes = Arrays.asList(
                //DataType.BIGDECIMAL
        );
        //castLookup.put(DataType.BIGINTEGER, bigIntegerToTypes);

        List<Byte> byteArrayToTypes = Arrays.asList(
                DataType.BOOLEAN,
                DataType.INTEGER,
                DataType.LONG,
                DataType.FLOAT,
                DataType.DOUBLE,
                DataType.DATETIME,
                DataType.CHARARRAY,
                //DataType.BIGINTEGER,
                //DataType.BIGDECIMAL,
                DataType.TUPLE,
                DataType.BAG,
                DataType.MAP
        );
        castLookup.put(DataType.BYTEARRAY, byteArrayToTypes);

    }


    /**
     * Computes a modified version of manhattan distance between
     * the two schemas: s1 & s2. Here the value on the same axis
     * are preferred over values that change axis as this means
     * that the number of casts required will be lesser on the same
     * axis.
     *
     * However, this function ceases to be a metric as the triangle
     * inequality does not hold.
     *
     * Each schema is an s1.size() dimensional vector.
     * The ordering for each axis is as defined by castLookup.
     * Unallowed casts are returned a dist of INFINITY.
     * @param s1
     * @param s2
     * @param s2Type
     * @return
     */
    private long fitPossible(Schema s1, Schema s2) {
        if(s1==null || s2==null) return INF;
        List<FieldSchema> sFields = s1.getFields();
        List<FieldSchema> fsFields = s2.getFields();
        
        long score = 0;
        int castCnt=0;
        for(int i=0;i<sFields.size();i++){
            FieldSchema sFS = sFields.get(i);
            if(sFS == null){
                return INF;
            }

            // if we have a byte array do not include it
            // in the computation of the score - bytearray
            // fields will be looked at separately outside
            // of this function
            if (sFS.type == DataType.BYTEARRAY)
                continue;
            
            //if we get to the vararg field (if defined) : take it repeatedly
            FieldSchema fsFS = fsFields.get(i);

            if(DataType.isSchemaType(sFS.type)){
                if(!FieldSchema.equals(sFS, fsFS, false, true))
                    return INF;
            }
            if(FieldSchema.equals(sFS, fsFS, true, true)) continue;
            if(!castLookup.containsKey(sFS.type))
                return INF;
            if(!(castLookup.get(sFS.type).contains(fsFS.type)))
                return INF;
            score += (castLookup.get(sFS.type)).indexOf(fsFS.type) + 1;
            ++castCnt;
        }
        return score * castCnt;
    }
    
//    private void insertCastsForUDF(UserFuncExpression func, Schema fromSch, Schema toSch, SchemaType toSchType)
//    throws FrontendException {
//        List<FieldSchema> fsLst = fromSch.getFields();
//        List<FieldSchema> tsLst = toSch.getFields();
//        List<LogicalExpression> args = func.getArguments();
//        int i=-1;
//        for (FieldSchema fFSch : fsLst) {
//            ++i;
//            //if we get to the vararg field (if defined) : take it repeatedly
//            FieldSchema tFSch = ((toSchType == SchemaType.VARARG) && i >= tsLst.size()) ? 
//                    tsLst.get(tsLst.size() - 1) : tsLst.get(i);
//            if (fFSch.type == tFSch.type) {
//                continue;
//            }
//            insertCast(func, Util.translateFieldSchema(tFSch), args.get(i));
//        }
//    }
//    
//    /**
//     * add cast to convert the input of exp
//     *  {@link LogicalExpression} arg to type toType
//     * @param exp
//     * @param toType
//     * @param arg
//     * @throws FrontendException
//     */
//    private void insertCast(LogicalExpression exp, byte toType, LogicalExpression arg)
//    throws FrontendException {
//        LogicalFieldSchema toFs = new LogicalSchema.LogicalFieldSchema(null, null, toType);
//        insertCast(exp, toFs, arg);
//    }
//
//    private void insertCast(LogicalExpression node, LogicalFieldSchema toFs,
//            LogicalExpression arg)
//    throws FrontendException {
//        collectCastWarning(node, arg.getType(), toFs.type, msgCollector);
//
//        CastExpression cast = new CastExpression(plan, arg, toFs);
//        try {
//            // disconnect cast and arg because the connection is already
//            // added by cast constructor and insertBetween call is going
//            // to do it again
//            plan.disconnect(cast, arg);
//            plan.insertBetween(node, cast, arg);
//        }
//        catch (PlanException pe) {
//            int errCode = 2059;
//            String msg = "Problem with inserting cast operator for " + node + " in plan.";
//            throw new TypeCheckerException(arg, msg, errCode, PigException.BUG, pe);
//        }
//        this.visit(cast);
//    }


//    /***
//     * Helper for collecting warning when casting is inserted
//     * to the plan (implicit casting)
//     *
//     * @param node
//     * @param originalType
//     * @param toType
//     */
//    static void collectCastWarning(Operator node,
//            byte originalType,
//            byte toType,
//            CompilationMessageCollector msgCollector
//    ) {
//        String originalTypeName = DataType.findTypeName(originalType) ;
//        String toTypeName = DataType.findTypeName(toType) ;
//        String opName= node.getClass().getSimpleName() ;
//        PigWarning kind = null;
//        switch(toType) {
//        case DataType.BAG:
//            kind = PigWarning.IMPLICIT_CAST_TO_BAG;
//            break;
//        case DataType.CHARARRAY:
//            kind = PigWarning.IMPLICIT_CAST_TO_CHARARRAY;
//            break;
//        case DataType.DOUBLE:
//            kind = PigWarning.IMPLICIT_CAST_TO_DOUBLE;
//            break;
//        case DataType.FLOAT:
//            kind = PigWarning.IMPLICIT_CAST_TO_FLOAT;
//            break;
//        case DataType.INTEGER:
//            kind = PigWarning.IMPLICIT_CAST_TO_INT;
//            break;
//        case DataType.LONG:
//            kind = PigWarning.IMPLICIT_CAST_TO_LONG;
//            break;
//        case DataType.BOOLEAN:
//            kind = PigWarning.IMPLICIT_CAST_TO_BOOLEAN;
//            break;
//        case DataType.DATETIME:
//            kind = PigWarning.IMPLICIT_CAST_TO_DATETIME;
//            break;
//        case DataType.MAP:
//            kind = PigWarning.IMPLICIT_CAST_TO_MAP;
//            break;
//        case DataType.TUPLE:
//            kind = PigWarning.IMPLICIT_CAST_TO_TUPLE;
//            break;
////        case DataType.BIGINTEGER:
////            kind = PigWarning.IMPLICIT_CAST_TO_BIGINTEGER;
////            break;
////        case DataType.BIGDECIMAL:
////            kind = PigWarning.IMPLICIT_CAST_TO_BIGDECIMAL;
////            break;
//        }
//        msgCollector.collect(originalTypeName + " is implicitly cast to "
//                + toTypeName +" under " + opName + " Operator",
//                MessageType.Warning, kind) ;
//    }
}
