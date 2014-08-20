package simpledb;

import java.util.*;
import simpledb.Tuple;

/**
 * Implements the iterator in HeapPage
 * Overrides Iterator<Tuple> to disable remove()
 */
public class HeapPageIterator implements Iterator<Tuple> {

	private static final long serialVersionUID = 1L;
	/**
	 * Tuples in a heap page
	 */
	ArrayList<Tuple> tuples;
	
	/**
	 * Iterator for usedTuples
	 */
	Iterator<Tuple> i;
	
	public HeapPageIterator(ArrayList<Tuple> usedTuples) {
		tuples = usedTuples;
		i = usedTuples.iterator();
	}
	
	@Override
	public boolean hasNext() {
		return i.hasNext();
	}

	@Override
	public Tuple next() {
		return i.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException(
				"remove not supported.");
	}

}
