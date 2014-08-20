package simpledb;

import java.io.Serializable;
import java.util.*;


/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * schema of the tuple
     */
    private TupleDesc schema;
    /**
     * the actual data of the tuple
     */
    private ArrayList<Field> data;
    /**
     * the corresponding record id of the tuple
     */
    private RecordId rid;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
    	// Error: invalid schema
        if (td == null || td.numFields() < 1)
        	throw new IllegalArgumentException(
        			"Tuple(TupleDesc): Invalid schema");
        
        // set schema
        schema = td;
       
        // initialize data fields
        data = new ArrayList<Field>();
        for (int i = 0; i < td.numFields(); i++)
        	data.add(i, null);
        
        // initialize rid
        rid = null;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        try {
        	data.set(i, f);
        } catch (IndexOutOfBoundsException e) {
        	throw new IndexOutOfBoundsException(
        			"setField(int, Field): invalid index i.");
        }
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        try {
        	return data.get(i);
        } catch (IndexOutOfBoundsException e) {
        	throw new IndexOutOfBoundsException(
				"getField(int): invalid index i.");
        }
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
    	String s = "";
        for (int i = 0; i < schema.numFields(); i++)
        {
        	if (i < schema.numFields()-1)
        		s += data.get(i).toString() + " ";
        	else
        		s += data.get(i).toString();
        }
        return s;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return data.iterator();
    }
    
    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
    	// Error: invalid schema
    	if (td == null || td.numFields() < 1)
        	throw new IllegalArgumentException(
        			"Tuple(TupleDesc): Invalid schema");
        	
    	// resize data
    	for (int i = schema.numFields(); i < td.numFields(); i++)
    		data.add(i, null);
    	for (int i = schema.numFields(); i > td.numFields(); i--)
    		data.remove(i);
        
    	schema = td;
    }
    
    
    // This changes the public interface!!!
    /**
     * Merge two Tuples into one, with t1's numFields + t2's numFields fields,
     * with the first t1's numFields coming from t1 and the remaining from t2.
     *
     * 
     * @param td1
     *            The Tuple with the first fields of the new Tuple
     * @param td2
     *            The Tuple with the last fields of the Tuple
     * @return the new Tuple
     */
    public static Tuple merge(Tuple t1, Tuple t2) {
    	// create new Tuple with merged TupleDesc
    	TupleDesc td1 = t1.getTupleDesc();
    	TupleDesc td2 = t2.getTupleDesc();
    	TupleDesc td = TupleDesc.merge(td1, td2);
    	Tuple tm = new Tuple(td);
    	
    	// fill new Tuple with data
    	int index = 0;
    	for (int i = 0; i < td1.numFields(); i++)
    	{
    		tm.setField(index, t1.getField(i));
    		index++;
    	}
    	for (int i = 0; i < td2.numFields(); i++)
    	{
    		tm.setField(index, t2.getField(i));
    		index++;
    	}
    
    	return tm;
    }
}
