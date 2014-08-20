package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private int field1; 
    private Predicate.Op op;
    private int field2;
    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        this.field1 = field1;
        this.field2 = field2;
        this.op = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
    	int numFields1 = t1.getTupleDesc().numFields();
    	int numFields2 = t2.getTupleDesc().numFields();
    	// Error: field index specified in constructor is out of bound
    	if (field1 >= numFields1 || field2 >= numFields2)
    		throw new IllegalArgumentException(
    				"filter(Tuple, Tuple): field index out of bound.");
    	
    	Field f1 = t1.getField(field1);
    	Field f2 = t2.getField(field2);
    	return f1.compare(op, f2);
    }
    
    public int getField1()
    {
        return field1;
    }
    
    public int getField2()
    {
        return field2;
    }
    
    public Predicate.Op getOperator()
    {
        return op;
    }
}
