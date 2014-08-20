package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId m_t;
    private DbIterator m_child;
    private TupleDesc m_schema;
    private Tuple m_tuple;
    private boolean m_called;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
       	m_t = t;
    	m_child = child;
 
    	Type[] fieldtype = new Type[1];
    	String[] fieldname = new String[1];
    	fieldtype[0] = Type.INT_TYPE;
    	fieldname[0] = "Number of Deleted Records";
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
    			Tuple delete = m_child.next();    //if child iterator has next tuple, delete it
    			try{
    				Database.getBufferPool().deleteTuple(m_t, delete);
    			}catch(IOException e)
    			{
    				e.printStackTrace();
    			}
    			num++;     //increment the number of deleted records by 1
    		}
    		Field count = new IntField(num);
    		m_tuple.setField(0, count);     //set value into the tuple
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
