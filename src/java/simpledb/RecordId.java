package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * the pageid of the page on which the tuple resides.
     */
    private PageId pid;
    /**
     * the tuple number of this tuple within the page.
     */
    private int tupleno;
    
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
    	return tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
    	if (o instanceof RecordId)
    	{
    		RecordId rid = (RecordId) o;
    		if (rid.pid.equals(this.pid) && rid.tupleno == this.tupleno)
    			return true;
    		else
    			return false;
    	}
    	else
    		return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
    	// concatenate pid.hashCode() and tupleno
    	Long codeLong = Long.parseLong(new Integer(pid.hashCode()).toString() + new Integer(tupleno).toString());
        return codeLong.hashCode();
    }

}
