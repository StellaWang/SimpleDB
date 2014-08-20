package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int m_gbfield;
    private int m_afield;
    private Type m_gbfieldtype;
    private Op m_what;
    private boolean m_nogrouping = false;
    private HashMap<Field, Integer> m_AggregateValue; //HashMap to keep aggregate value for each grouped field
    private HashMap<Field, Integer> m_CountNum;   //HashMap to record the number of elements in each grouped field
    private ArrayList<Tuple> m_results;
    private String m_afieldname = "";
    private String m_gbfieldname = "";
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	m_gbfield = gbfield;
    	m_afield = afield;
    	m_gbfieldtype = gbfieldtype;
    	m_what = what;
    	m_AggregateValue = new HashMap <Field, Integer>();
    	m_CountNum = new HashMap <Field, Integer>();
    	if(gbfield == Aggregator.NO_GROUPING)  //if NO_GROUPING, set the m_nogrouping flag to true
    		m_nogrouping = true;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field GroupField;    //get the Field of the group-by field in the tuple
    	int AggregateValue;
    	int CountNum;
    	IntField AggField = (IntField)tup.getField(m_afield);
    	int value = AggField.getValue();   //get the value in the aggregate field
    	
    	//if NO_GROUPING, create a new group-by field to record not grouped values 
    	if(m_nogrouping)
    		GroupField = new IntField(Aggregator.NO_GROUPING); 
    	//if grouped, get the Field of the group-by field in the tuple
    	else
    	{
    		GroupField = tup.getField(m_gbfield);
    		m_gbfieldname = tup.getTupleDesc().getFieldName(m_gbfield);
    	}
    	
    	m_afieldname = tup.getTupleDesc().getFieldName(m_afield);
    	
    	//If GroupField doesn't exist in the HashMap, insert a new <Field, Integer> pair
    	if(!m_AggregateValue.containsKey(GroupField))
    		m_AggregateValue.put(GroupField, value);
    	
    	if(!m_CountNum.containsKey(GroupField))
    		m_CountNum.put(GroupField, 0);
    	
    	//Get the aggregate value and number of elements in the hashmap
    	AggregateValue = m_AggregateValue.get(GroupField);
    	CountNum = m_CountNum.get(GroupField);
    	
    	if(m_what == Op.MAX)
    	{
    		if(value >= AggregateValue){
    			AggregateValue = value;
    		}
    		
    		m_AggregateValue.put(GroupField, AggregateValue);
    	}
    	
    	if(m_what == Op.MIN)
    	{
    		if(value <= AggregateValue){
    			AggregateValue = value;
    		}
    		
    		m_AggregateValue.put(GroupField, AggregateValue);
    	}
    	
    	if(m_what == Op.COUNT)
    	{
    		CountNum++;
    		m_CountNum.put(GroupField, CountNum);
    	}
    	
    	if(m_what == Op.SUM)
    	{
    		//If this tuple is the first tuple inserted into this group, don't add it to itself
    		if(CountNum != 0)     
    			AggregateValue += value;
    		
    		CountNum++;
    		m_AggregateValue.put(GroupField, AggregateValue);
    		m_CountNum.put(GroupField, CountNum);
    	}
    	
    	if(m_what == Op.AVG)
    	{
    		if(CountNum != 0)
    			AggregateValue += value;
    		
    		CountNum++;
    		m_AggregateValue.put(GroupField, AggregateValue);
    		m_CountNum.put(GroupField, CountNum);
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	Type[] FieldType;
    	String[] FieldName;
    	if(m_nogrouping){
    		FieldType = new Type[1];
    		FieldName = new String[1];
    		FieldType[0] = Type.INT_TYPE;
    		FieldName[0] = m_afieldname;
    	}
    	else
    	{
    		FieldType = new Type[2];
    		FieldName = new String[2];
    		FieldType[0] = m_gbfieldtype;
    		FieldName[0] = m_gbfieldname;
    		FieldType[1] = Type.INT_TYPE;
    		FieldName[1] = m_afieldname;
    	}
    	TupleDesc schema = new TupleDesc(FieldType, FieldName);
    	
    	m_results = new ArrayList<Tuple>();
    	
    	if(m_nogrouping)
    	{
    		if(m_what == Op.COUNT)
    		{
    			for(Field GroupField: m_CountNum.keySet())
    			{
    				int value = m_CountNum.get(GroupField);
    				Tuple t = new Tuple(schema);
        			t.setField(0, new IntField(value));
        			m_results.add(t);
    			}
    		}
    		else if(m_what == Op.AVG)
    		{
    			for (Field GroupField: m_AggregateValue.keySet())
    			{
    				int value = (int)(m_AggregateValue.get(GroupField)/m_CountNum.get(GroupField));
    				Tuple t = new Tuple(schema);
        			t.setField(0, new IntField(value));
        			m_results.add(t);
    			}
    		}
    		else {
    			for (Field GroupField: m_AggregateValue.keySet())
    			{
    				int value = m_AggregateValue.get(GroupField);
    				Tuple t = new Tuple(schema);
        			t.setField(0, new IntField(value));
        			m_results.add(t);
    			}
			}
    	}
    	else
    	{
    		if(m_what == Op.COUNT)
    		{
    			for(Field GroupField: m_CountNum.keySet())
    			{
    				int value = m_CountNum.get(GroupField);
    				Tuple t = new Tuple(schema);
    				t.setField(0, GroupField);
        			t.setField(1, new IntField(value));
        			m_results.add(t);
    			}
    		}
    		else if (m_what == Op.AVG)
    		{
    			for (Field GroupField: m_AggregateValue.keySet())
    			{
    				int value = (int)(m_AggregateValue.get(GroupField)/m_CountNum.get(GroupField));
    				Tuple t = new Tuple(schema);
    				t.setField(0, GroupField);
        			t.setField(1, new IntField(value));
        			m_results.add(t);
    			}
    		}
    		else
    		{
    			for (Field GroupField: m_AggregateValue.keySet())
    			{
    				int value = m_AggregateValue.get(GroupField);
    				Tuple t = new Tuple(schema);
        			t.setField(0, GroupField);
        			t.setField(1, new IntField(value));
        			m_results.add(t);
    			}
			}
    	}
    return new TupleIterator(schema, m_results);
    }
}
