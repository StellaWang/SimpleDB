package simpledb;

import java.util.*;

/**
 * Implements a DbFileIterator that iterates all tuples in a HeapFile
 * by wrapping HeapPage.iterator().
 */
public class HeapFileIterator implements DbFileIterator {
	
	private static final long serialVersionUID = 1L;
	Iterator<Tuple> i = null;
	/**
	 * HeapFile to iterate through
	 */
	HeapFile f; 
	TransactionId tid;
	/**
	 * Current page number in the HeapFile
	 */
	int pgNo;
	
	public HeapFileIterator(HeapFile f, TransactionId tid) {
		this.f = f;
		this.tid = tid;
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		try {
			// Use HeapPage.iterator() in the first page
			pgNo = 0;
			i = PageToTuples(pgNo).iterator();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		} catch (DbException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		// iterator not opened 
		if (i == null)
			return false;
		// current page has more tuples
		if (i.hasNext())
			return true;
		// otherwise, check the next page
		// if next page is empty, we've reached EOF
		if (pgNo < f.numPages()-1)
			return PageToTuples(pgNo+1).size() != 0;
		return false;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException,
			NoSuchElementException {
		// iterator not opened
		if (i == null)
			throw new NoSuchElementException(
					"next(): iterator not opened.");
		// if current page has no more tuples 
		// move to the next page if possible
		if (!i.hasNext() && pgNo < f.numPages()-1)
		{
			pgNo++;
			i = PageToTuples(pgNo).iterator();
		}	
		// if current page has more tuples
		if (i.hasNext())
			return i.next();
		throw new NoSuchElementException(
				"next(): reached end of page.");
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}
	
	@Override
	public void close() {
		i = null;
	}
	
	/**
	 * Helper function to construct an ArrayList of tuples form a HeapPage.
	 * 
	 * @param pgNo page number of the HeapPage.
	 * @return An ArrayList of tuples that reside in this page.
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	private ArrayList<Tuple> PageToTuples(int pgNo) throws DbException, TransactionAbortedException {
		try {
			// get the HeapPage with page number pgNo
			PageId pid = new HeapPageId(f.getId(), pgNo);
			Page p = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
			HeapPage hp = (HeapPage) p;
			
			// add tuples in the page to an ArrayList
			Iterator<Tuple> it = hp.iterator();
			ArrayList<Tuple> tuples = new ArrayList<Tuple>();
			while (it.hasNext())
				tuples.add(it.next());
			return tuples;
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		} catch (DbException e) {
			e.printStackTrace();
		}
		// we should never get here
		return null;
	}
}
