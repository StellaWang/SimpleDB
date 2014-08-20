package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.org.apache.bcel.internal.generic.NEW;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private Integer m_tableid;
    private Integer m_ioCostPerPage;
    //String for field name, Integer for static value
    private HashMap<String, Integer> m_Min;
    private HashMap<String,	Integer> m_Max;
    //String for field name, Object for histograms of that field
    private HashMap<String, Object> m_histograms;
    private HeapFile m_heapFile;
    private TupleDesc m_tupleDesc;
    private DbFileIterator m_iterator;
    private Transaction m_transaction;
    private TransactionId m_tid;
    private Integer m_numofTuples;
    
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	m_tableid = tableid;
    	m_ioCostPerPage = ioCostPerPage;
    	m_Min = new HashMap<String, Integer>();
    	m_Max = new HashMap<String, Integer>();
    	m_histograms = new HashMap<String, Object>();
        m_heapFile = (HeapFile)Database.getCatalog().getDatabaseFile(m_tableid);
        m_tupleDesc = m_heapFile.getTupleDesc();
        m_transaction = new Transaction();
        m_tid = m_transaction.getId();
        m_iterator = m_heapFile.iterator(m_tid);
        
        try {
			m_iterator.open();
			Integer NumOfFields = m_tupleDesc.numFields();
			
			//Create a histogram for each field in the tuple
			for(int i = 0; i < NumOfFields; i++)
			{
				String fieldname = m_tupleDesc.getFieldName(i);
				Type fieldType = m_tupleDesc.getFieldType(i);	
				
				//If fieldtype is integer, need to get min and max value in the field
				if(fieldType.equals(Type.INT_TYPE))
				{
					while (m_iterator.hasNext())
					{
						Tuple tuple = m_iterator.next();
						Integer value = ((IntField)tuple.getField(i)).getValue();
						if(!m_Min.containsKey(fieldname) || value < m_Min.get(fieldname))
						{
							m_Min.put(fieldname, value);
						}
						if(!m_Max.containsKey(fieldname) || value > m_Max.get(fieldname))
						{
							m_Max.put(fieldname, value);
						}
					}
					m_iterator.rewind();
					
					IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, m_Min.get(fieldname), m_Max.get(fieldname));
					m_histograms.put(fieldname, intHistogram);
				}
				//If fieldtype is string, no need to get min and max value in the field
				else {
					StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
					m_histograms.put(fieldname, stringHistogram);
				}
			}
			
			//Add values into the histogram of each field of the table
			for(int i = 0; i < NumOfFields; i++)
			{
				String fieldname = m_tupleDesc.getFieldName(i);
				Type fieldType = m_tupleDesc.getFieldType(i);
				m_numofTuples = 0;
				
				if(fieldType.equals(Type.INT_TYPE))
				{
					while (m_iterator.hasNext())
					{
						m_numofTuples++;
						Tuple tuple = m_iterator.next();
						Integer value = ((IntField)tuple.getField(i)).getValue();
						IntHistogram intHistogram = (IntHistogram)m_histograms.get(fieldname);
						intHistogram.addValue(value);
					}
					m_iterator.rewind();
				}
				//If fieldtype is string
				else {
					while (m_iterator.hasNext())
					{
						m_numofTuples++;
						Tuple tuple = m_iterator.next();
						String value = ((StringField)tuple.getField(i)).getValue();
						StringHistogram stringHistogram = (StringHistogram)m_histograms.get(fieldname);
						stringHistogram.addValue(value);
					}
					m_iterator.rewind();
				}
			}
			
			m_iterator.close();
			
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return m_heapFile.numPages()*m_ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) Math.ceil(m_numofTuples*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
    	String fieldname = m_tupleDesc.getFieldName(field);
    	Type fieldType = m_tupleDesc.getFieldType(field);

    	//If it's an equality predicate
    	if(op == Predicate.Op.EQUALS)
    	{
    		if (fieldType == Type.INT_TYPE) {
    			IntHistogram intHistogram = (IntHistogram) m_histograms.get(fieldname);
    			return intHistogram.avgSelectivity();
    		}
    		else //fieldtype is string
    		{
    			StringHistogram stringHistogram = (StringHistogram)	m_histograms.get(fieldname);
    			return stringHistogram.avgSelectivity();
    		}
    	}
    	else {
    		//If it's not an equality predicate, just return a made-up value
    		return 0.5;
    	}
    }
    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
    	String fieldname = m_tupleDesc.getFieldName(field);
    	Type fieldType = m_tupleDesc.getFieldType(field);
    	if (fieldType == Type.INT_TYPE) {
			IntHistogram intHistogram = (IntHistogram) m_histograms.get(fieldname);
			return intHistogram.estimateSelectivity(op, ((IntField)constant).getValue());
		}
    	else //fieldtype is string
    	{
		    StringHistogram stringHistogram = (StringHistogram)	m_histograms.get(fieldname);
		    return stringHistogram.estimateSelectivity(op, ((StringField)constant).getValue());
		}
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return m_numofTuples;
    }

}
