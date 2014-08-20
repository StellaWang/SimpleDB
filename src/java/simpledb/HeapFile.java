package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	/**
	 * the file that stores the on-disk backing store for this heap file.
	 */
	File f;
	/**
	 * schema of the table stored in this DbFile.
	 */
	private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // Error: page does not exist in this file
    	if (pid.getTableId() != getId())
    		throw new IllegalArgumentException(
    				"readPage(PageId): page does not exist in this file.");
    	
    	try {
    		// create a RandomAccessFile and seek to the correct position
    		RandomAccessFile file = new RandomAccessFile(f, "r");
    		int size = BufferPool.PAGE_SIZE;
    		file.seek(pid.pageNumber()*size);
    		
    		// read BufferPool.PAGE_SIZE bytes into a byte array
    		byte [] page = new byte[size];
    		file.read(page, 0, size);
    		file.close();
    		
    		// create a HeapPage form the byte array
    		return new HeapPage((HeapPageId)pid, page);
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    	return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	
    	try {
    		// create a RandomAccessFile and seek to the correct position
    		RandomAccessFile file = new RandomAccessFile(f, "rw");
    		int size = BufferPool.PAGE_SIZE;
    		file.seek(page.getId().pageNumber()*size);
    		
    		// write page to file 
    		file.write(page.getPageData(), 0, size);
    		file.close();
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(f.length()*1.0 / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	
    	ArrayList<Page> pages = new ArrayList<Page> ();
        try {
        	HeapPage hp = (HeapPage) getNextPageWithEmptySlot(tid);
        	if (hp != null) { // at least one HeapPage with empty slot
        		hp.insertTuple(t);
        		pages.add(hp);
        	} else { // all pages are full
        		HeapPageId pid = new HeapPageId(getId(), numPages());
        		HeapPage newHp = new HeapPage(pid, HeapPage.createEmptyPageData());
        		newHp.insertTuple(t);
        		writePage(newHp);
        		pages.add(newHp);
        	}
        } catch (DbException e) {
        	e.printStackTrace();
        } catch (IOException e ) {
        	e.printStackTrace();
        } catch (TransactionAbortedException e) {
        	e.printStackTrace();
        }
    	
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	ArrayList<Page> pages = new ArrayList<Page> ();
    	try {
    		PageId pid = t.getRecordId().getPageId();
    		HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    		hp.deleteTuple(t);
    		pages.add(Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY));
    	} catch (DbException e) {
        	e.printStackTrace();
    	} catch (TransactionAbortedException e) {
        	e.printStackTrace();
        }
    	
    	return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // see HeapFileIterator.java
        return new HeapFileIterator(this, tid);
    }
    
    /**
     * Return the next page with at least one empty slot. Return null if all pages are full.
     * Used by insertTuple(TransactionId, Tuple).
     * 
     * @param tid Transaction id 
     * @return Next page with empty slot. Null if all pages are full.
     * @throws DbException
     * @throws TransactionAbortedException
     */
    private Page getNextPageWithEmptySlot(TransactionId tid) throws DbException,
    		TransactionAbortedException {
    	try {
    		int tbid = getId();
    		// read a HeapPage and determine whether it has empty slots
			for (int i = 0; i < numPages(); i++) {
    			HeapPageId pid = new HeapPageId(tbid, i);
    			HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    			if (hp.getNumEmptySlots() > 0) 
    				return hp;
			}
    	} catch (DbException e) {
    		e.printStackTrace();
    	} catch (TransactionAbortedException e) {
    		e.printStackTrace();
    	}
    	return null;
    }
}

