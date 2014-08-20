package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    
    private DbIterator m_child;
    private int m_afield;
    private int m_gfield;
    private Aggregator.Op m_aop;
    private Type m_gbfieldtype;
    private Type m_afieldtype;
    private Aggregator m_Aggregator;
    private DbIterator m_AggregatorIterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    	m_child = child;
    	m_afield = afield;
    	m_gfield = gfield;
    	m_aop = aop;
    	//record the type of the group by field
    	if(m_gfield == Aggregator.NO_GROUPING){
    		m_gbfieldtype = null;
    	}
    	else
    	{
    		m_gbfieldtype = m_child.getTupleDesc().getFieldType(gfield);
    	}
    	//record type of the aggregate field
    	m_afieldtype = m_child.getTupleDesc().getFieldType(afield);
    	if(m_afieldtype == Type.INT_TYPE){
    		//create different types of aggregator according to the type of aggregate field
    		m_Aggregator = new IntegerAggregator(m_gfield, m_gbfieldtype, m_afield, m_aop);
    	}
    	else
    	{
    		m_Aggregator = new StringAggregator(m_gfield, m_gbfieldtype, m_afield, m_aop);
    	}
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	return m_gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
    if (m_gfield == Aggregator.NO_GROUPING)
    	return null;
    else
    	return m_child.getTupleDesc().getFieldName(m_gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return m_afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return m_child.getTupleDesc().getFieldName(m_afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return m_aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	m_child.open();
    	while(m_child.hasNext())
    	{
    		m_Aggregator.mergeTupleIntoGroup(m_child.next());
    	}
    	//open the iterator of the aggregator
    	m_AggregatorIterator = m_Aggregator.iterator();
    	m_AggregatorIterator.open();
    	
    super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
    if(m_AggregatorIterator.hasNext()){
    	Tuple t = m_AggregatorIterator.next();
    	return t;
    }
    else
    	return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    	m_child.rewind();
    	m_AggregatorIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
    //create the TupleDesc with required format
       	Type[] FieldType;
    	String[] FieldName;
    	if(m_afield == Aggregator.NO_GROUPING)
    	{
    		FieldType = new Type[1];
    		FieldName = new String[1];
    		FieldType[0] = m_child.getTupleDesc().getFieldType(m_afield);
    		FieldName[0] = m_aop.toString()+"("+m_child.getTupleDesc().getFieldName(m_afield)+")";
    		return new TupleDesc (FieldType,FieldName);
    	}
    	else
    	{
    		FieldType = new Type[2];
    		FieldName = new String[2];
    		FieldType[0] = m_child.getTupleDesc().getFieldType(m_gfield);
    		FieldName[0] = m_child.getTupleDesc().getFieldName(m_gfield);
    		FieldType[1] = m_child.getTupleDesc().getFieldType(m_afield);
    		FieldName[1] = m_aop.toString()+"("+m_child.getTupleDesc().getFieldName(m_afield)+")";
    		return new TupleDesc (FieldType,FieldName);
    	}
    }

    public void close() {
	// some code goes here
    	m_child.close();
    	m_AggregatorIterator.close();
    	
    super.close();
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
	return new DbIterator[] { this.m_child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
    		this.m_child = children[0];
    }
    
}
