/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.surus.linkanalysis;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 *
 * @author jasonr
 */
public class Predicate<T> {
 
    private final Expression left;
    private final Expression right;
    private final String operand;
    
    public Predicate(Expression left, Expression right, String operand) {
        this.left = left;
        this.right = right;
        this.operand = operand;
    }
    
    public Expression getLeftExpression() {
    	return this.left;
    }
    
    public Expression getRightExpression() {
    	return this.right;
    }
    
    public String getOperand() {
    	return this.operand;
    }
    
    public static Predicate parse(String unparsedPredicate, Schema bagSchema) throws Throwable {
    	
        // Eric's awesome regex match
        Pattern p = Pattern.compile("(\\w+|\\'.*\\')\\s*(=|!=|<|>|<=|>=)\\s*(\\w+|\\'.*\\')");
        Matcher m = p.matcher(unparsedPredicate);
        
        if (m.find()) {
            String leftExpressionValue = m.group(1);
            String predicateOperator = m.group(2);
            String rightExpressionValue = m.group(3);

            Boolean leftIsColumn = !leftExpressionValue.contains("'");
            Boolean rightIsColumn = !rightExpressionValue.contains("'");

            leftExpressionValue  = leftExpressionValue.replace("'", "");
            rightExpressionValue = rightExpressionValue.replace("'","");

            Expression lExpression;
            if (leftIsColumn) {
                Integer i = bagSchema.getPosition(leftExpressionValue);
                lExpression = new TupleExpression(leftExpressionValue, i);
            } else {
                lExpression = new TupleExpression(leftExpressionValue);
            }
            
            Expression rExpression;
            if (rightIsColumn) {
                Integer i = bagSchema.getPosition(rightExpressionValue);
                rExpression = new TupleExpression(rightExpressionValue, i);
            } else {
                rExpression = new TupleExpression(rightExpressionValue);
            }
            return new Predicate(lExpression, rExpression, predicateOperator);

        }
        throw new RuntimeException("Error parsing predicate: " + unparsedPredicate);

    }
    
    boolean apply(T t) throws ExecException {
        
        Object lhs = left.Evaluate(t);
        Object rhs = right.Evaluate(t);
        
        if (lhs == null || rhs == null) {
            return false;
        }
        
        Integer lexi = null;
        
        if (lhs instanceof Double || rhs instanceof Double) {
            Double l;
            Double r;
            
            if (lhs instanceof String) {
                l = Double.parseDouble((String)lhs);
            } else {
                l = (Double)lhs;
            }
            if (rhs instanceof String) {
                r = Double.parseDouble((String)rhs);
            } else {
                r = (Double)rhs;
            }
            
            lexi = l.compareTo(r);
            
        } else if (lhs instanceof Float || rhs instanceof Float) {
            Float l;
            Float r;
            if (lhs instanceof String) {
                l = Float.parseFloat((String)lhs);
            } else {
                l = (Float)lhs;
            }
            if (rhs instanceof String) {
                r = Float.parseFloat((String)rhs);
            } else {
                r = (Float)rhs;
            }
            lexi = l.compareTo(r);
        } else if (lhs instanceof Long || rhs instanceof Long) {
            Long l;
            Long r;
            if (lhs instanceof String) {
                l = Long.parseLong((String)lhs);
            } else {
                l = (Long)lhs;
            }
            if (rhs instanceof String) {
                r = Long.parseLong((String)rhs);
            } else {
                r = (Long)rhs;
            }
            lexi = l.compareTo(r);
        } else if (lhs instanceof Integer || rhs instanceof Integer) {
            Integer l;
            Integer r;
            if (lhs instanceof String) {
                l = Integer.parseInt((String)lhs);
            } else {
                l = (Integer)lhs;
            }
            if (rhs instanceof String) {
                r = Integer.parseInt((String)rhs);
            } else {
                r = (Integer)rhs;
            }
            lexi = l.compareTo(r);
        } else if (lhs instanceof String && rhs instanceof String) {
            lexi = ((String)lhs).compareTo((String)rhs);
        } else {
            throw new ExecException("Could not compare objects of type " + lhs.getClass().getName() + " and type " + rhs.getClass().getName() + ".");
        }

        // System.out.println(lhs + " " + rhs + " " + lexi);

        
        if ("=".equals(operand)) {
            
            return lexi == 0;
            
        } else if ("!=".equals(operand)) {
            
            return lexi != 0;
            
        } else if ("<".equals(operand)) {
            
            return lexi < 0;
            
        } else if ("<=".equals(operand)) {
            
            return lexi <= 0;
            
        } else if (">".equals(operand)) {
            
            return lexi > 0;
            
        } else if (">=".equals(operand)) {
            
            return lexi >= 0;
        }
        
        throw new ExecException("Unrecognized operand: " + operand + ".  Supported operands are =,!=,<,>,<=,>=");
    }
    
    @Override
    public String toString(){
    	return this.left + " " + this.operand + " " + this.right;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((left == null) ? 0 : left.hashCode());
		result = prime * result + ((operand == null) ? 0 : operand.hashCode());
		result = prime * result + ((right == null) ? 0 : right.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Predicate other = (Predicate) obj;
		if (left == null) {
			if (other.left != null)
				return false;
		} else if (!left.equals(other.left))
			return false;
		if (operand == null) {
			if (other.operand != null)
				return false;
		} else if (!operand.equals(other.operand))
			return false;
		if (right == null) {
			if (other.right != null)
				return false;
		} else if (!right.equals(other.right))
			return false;
		return true;
	}

}
