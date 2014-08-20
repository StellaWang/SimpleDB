package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFile DbFile;
    private DbFileIterator DbIt;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
    	this.tid = tid;
    	this.tableid = tableid;
    	this.tableAlias = tableAlias;
    	this.DbFile = Database.getCatalog().getDatabaseFile(tableid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
    	//reset the tableid and tableAlias with new tableid and tableAlias
    	this.tableid = tableid;
    	this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
    	//get the iterator of the DbFile
    	//this.DbIt = this.DbFile.iterator(tid);
    	DbIt = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    	DbIt.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        //get the field name of the fields in the tupledesc
    	//add the prefix and create a new tupledesc
    	TupleDesc tupleDesc = DbFile.getTupleDesc();
    	int size = tupleDesc.numFields();
    	Type [] typeAr = new Type [size];
    	String [] fieldAr = new String [size];
    	for (int i = 0; i < size; i++)
    	{
    		typeAr[i] = tupleDesc.getFieldType(i);
    		fieldAr[i] = tableAlias + "." + tupleDesc.getFieldName(i);
    	}
    	
        return new TupleDesc(typeAr, fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        //if iterator has next element return true, else return false
    	if (DbIt != null)
    		return DbIt.hasNext();
    	return false;
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        //if iterator has next element, return the element, else throw exception
    	if(DbIt.hasNext())
    	{
    		return DbIt.next();
    	}
    	else
    		throw new NoSuchElementException("No such Tuple.");
    }

    public void close() {
        //clear this iterator
    	DbIt = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // Resets the iterator to the start.
        // throws DbException when rewind is unsupported.
    	DbIt.close();
    	DbIt.open();
    }
}