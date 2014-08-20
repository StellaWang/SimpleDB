package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    
    /**
     * List of TDItems of a schema
     */
    private ArrayList<TDItem> TDList;
    
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return TDList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	// Error: typeArr is empty
    	if (typeAr.length < 1)
    		throw new IllegalArgumentException(
    			"TupleDesc(Type[], String[]): typeAr must contain at least one entry.");
    	
    	// Error: typeAr and fieldAr have difference lengths
    	if (typeAr.length != fieldAr.length)
    		throw new IllegalArgumentException(
				"TupleDesc(Type[], String[]): typeAr and fieldAr must contain same number of elements.");
    	
    	// Create new TupleDesc
    	TDList = new ArrayList<TDItem>();
    	for (int i = 0; i < typeAr.length; i++)
    		TDList.add(new TDItem(typeAr[i], fieldAr[i]));
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	// Error: typeArr is empty
    	if (typeAr.length < 1)
    		throw new IllegalArgumentException(
    			"TupleDesc(Type[]): typeAr must contain at least one entry.");
    	
    	// Create new TupleDesc
    	TDList = new ArrayList<TDItem>();
    	for (int i = 0; i < typeAr.length; i++)
    		TDList.add(new TDItem(typeAr[i], ""));
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return TDList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        try {
        	return TDList.get(i).fieldName;
        } catch(IndexOutOfBoundsException e) {
        	throw new NoSuchElementException();
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	try {
        	return TDList.get(i).fieldType;
        } catch(IndexOutOfBoundsException e) {
        	throw new NoSuchElementException();
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < TDList.size(); i++)
        	if (this.getFieldName(i).equals(name))
        		return i;
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */

    public int getSize() {
        int size = 0;
        for (int i = 0; i < TDList.size(); i++)
        {
        	if (this.getFieldType(i).equals(Type.INT_TYPE))
        		size += Type.INT_TYPE.getLen();
        	else
        		size += Type.STRING_TYPE.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	// Allocate and fill merged typeAr and fieldAr
    	int size = td1.numFields()+td2.numFields();
    	Type [] typeAr = new Type[size];
    	String[] fieldAr = new String[size];
    	
    	int index = 0;
    	for (int i = 0; i < td1.numFields(); i++)
    	{
    		typeAr[index] = td1.getFieldType(i);
    		fieldAr[index] = td1.getFieldName(i);
    		index++;
    	}
    	for (int i = 0; i < td2.numFields(); i++)
    	{
    		typeAr[index] = td2.getFieldType(i);
    		fieldAr[index] = td2.getFieldName(i);
    		index++;
    	}
    	
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	// if o is of type TupleDesc
        if (o instanceof TupleDesc)
        {
        	TupleDesc t = (TupleDesc) o;
        	// if o has the same number of fields
        	if (this.numFields() == t.numFields())
        	{
        		for (int i = 0; i < this.numFields(); i++)
        			if (!this.getFieldType(i).equals(t.getFieldType(i)))
        				return false;
        		return true;
        	}
        	else 
        		return false;
        }
        else 
        	return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	String s = "";
        for (int i = 0; i < this.numFields(); i++)
        {
        	s += this.getFieldType(i).toString() + "(" + this.getFieldName(i) + ")";
        	if (i < this.numFields()-1)
        		s += ", ";
        }
        return s;
    }
}
