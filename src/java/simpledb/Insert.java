package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId m_t;
    private DbIterator m_child;
    private int m_tableid;
    private TupleDesc m_schema;
    private Tuple m_tuple;
    private boolean m_called;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	m_t = t;
    	m_child = child;
    	m_tableid = tableid;
    	
    	TupleDesc childschema = m_child.getTupleDesc();
    	TupleDesc tableschema = Database.getCatalog().getTupleDesc(tableid);
    	if(!childschema.equals(tableschema))
    		throw new DbException("Different TupleDesc");
    	//create a tuple to store the number of inserted records
    	Type[] fieldtype = new Type[1];
    	String[] fieldname = new String[1];
    	fieldtype[0] = Type.INT_TYPE;
    	fieldname[0] = "Number of Inserted Records";
    	m_schema = new TupleDesc(fieldtype,fieldname);
    	m_tuple = new Tuple(m_schema);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return m_schema;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	m_child.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	m_child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	m_child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	try{
    	if(m_called)
    		return null;
    	else {
    		m_called = true;
    		int num = 0;
    		while(m_child.hasNext())
    		{
    			Tuple insert = m_child.next(); //if the child iterator has next iterator, insert it
    			try{
    				Database.getBufferPool().insertTuple(m_t, m_tableid, insert);
    			}catch(IOException e)
    			{
    				e.printStackTrace();
    			}
    			num++;     //increment the number of inserted records by 1
    		}
    		Field count = new IntField(num);
    		m_tuple.setField(0, count);    //set the value into the tuple
    	}
    	}
    	catch(DbException e)
    	{
    		e.printStackTrace();
    	}
    	catch(TransactionAbortedException e)
    	{
    		e.printStackTrace();
    	}
    	return m_tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{this.m_child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	this.m_child = children[0];
    }
}
