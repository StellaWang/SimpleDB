package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate p;
    private DbIterator child;
    private TupleDesc td;
    private ArrayList<Tuple> childTups = new ArrayList<Tuple>();
    private Iterator<Tuple> it;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
    	this.p = p;
    	this.child = child;
    	td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        return p;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	child.open();
    	// load and filter all the tuples
    	while (child.hasNext()) {
    		Tuple t = ((Tuple) child.next());
    		if (p.filter(t))
    			childTups.add(t);
    	}
    	it = childTups.iterator();
        super.open();
    }

    public void close() {
    	child.close();
        super.close();
        it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	it = childTups.iterator();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (it != null && it.hasNext()) {
            return it.next();
        } else
            return null;
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	this.child = children[0];
    }

}
