package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private HashMap<PageId, Page> bufferpool;
    /*
     * LRU queue for pages in buffer pool. 
     * Map PageId to a sequence number.
     * Pages with smaller sequence numbers are more recently requested. 
     */
    private HashMap<PageId, Integer> LRUqueue;
    private int numPages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        bufferpool = new HashMap<PageId, Page>();
        LRUqueue = new HashMap<PageId, Integer>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	if(bufferpool.containsKey(pid)) {
    		requestPage(pid);
    		return bufferpool.get(pid);
    	}
    	else
    	{
    		Integer TableId = pid.getTableId();
    	    DbFile file = Database.getCatalog().getDatabaseFile(TableId);
    	    Page PageRead = file.readPage(pid);
    	    if(bufferpool.size() == numPages)
    	    {
    	    	//eviction
    	    	evictPage();
    	    }
    	    requestPage(pid);
    	    bufferpool.put(pid, PageRead);
    	    return PageRead;
    	}
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	
    	try {
        	HeapFile hp = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        	ArrayList<Page> pages = hp.insertTuple(tid, t);
        	
        	// mark affected pages dirty
        	// update corresponding pages in bufferpool
        	for (Page p : pages) {
        		p.markDirty(true, tid);
        		bufferpool.put(p.getId(), p);
        	}
        } catch (DbException e) {
        	e.printStackTrace();
        } catch (IOException e ) {
        	e.printStackTrace();
        } catch (TransactionAbortedException e) {
        	e.printStackTrace();
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	
    	try {
    		int tableId = t.getRecordId().getPageId().getTableId();
        	HeapFile hp = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        	ArrayList<Page> pages = hp.deleteTuple(tid, t);
        	
        	// mark affected pages dirty
        	for (Page p : pages) {
        		p.markDirty(true, tid);
        	}
        } catch (DbException e) {
        	e.printStackTrace();
        } catch (TransactionAbortedException e) {
        	e.printStackTrace();
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

    	for (PageId pid : bufferpool.keySet())
    		flushPage(pid);
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	
    	try {
    		// get the HeapFile the page resides in
    		int tableId = ((HeapPageId) pid).getTableId();
    		HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
    		Page p = bufferpool.get(pid);
    		
    		// write page to file and mark it clean
    		hf.writePage(p);
    		p.markDirty(false, null);
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	
    	int seqnum = -1;
    	PageId pidToEvict = null;
    	// find the page with the highest sequence number to evict
    	for (PageId pid : LRUqueue.keySet()) {
    		int num = LRUqueue.get(pid);
    		if (num > seqnum) {
    			num = seqnum;
    			pidToEvict = pid;
    		}
    	}
    	// flush the page to disk 
    	// remove it from bufferpool
    	try {
    		flushPage(pidToEvict);
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    	bufferpool.remove(pidToEvict);
    	LRUqueue.remove(pidToEvict);
    }

    /**
     * Increment the sequence number of every page in bufferpool.
     * Put the newly requested page to the back of the queue.
     * @param pid The PageID of the requested page.
     */
    private void requestPage(PageId pid) {
    	for (PageId p : LRUqueue.keySet()) {
    		int seqnum = LRUqueue.get(p) + 1;
    		LRUqueue.put(p, seqnum);
    	}
    	LRUqueue.put(pid, 0);
    }
}