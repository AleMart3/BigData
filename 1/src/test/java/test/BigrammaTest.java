package test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import bigram.Bigramma;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class BigrammaTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public BigrammaTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( BigrammaTest.class );
    }

    /**
     * Rigourous Test :-)
     * @throws ParseException 
     */
    public void testApp() throws ParseException
    {
        
    	Text t1= new Text("la");
    	Text t2= new Text("panca");
    	
    	Text t3= new Text("panca");
    	Text t4= new Text("la");
    	
    	
    	Bigramma b1= new Bigramma(t1,t2);
    	Bigramma b2= new Bigramma(t3,t4);
    	
    	System.out.println(b1.equals(b2));
    	System.out.println(b1.compareTo(b2));
    	List<IntWritable> list = new ArrayList<IntWritable>();
    	
    	list.add(new IntWritable(1));
    	list.add(new IntWritable(2));
    	System.out.println(list);
    	
    	List<IntWritable> list2 = new ArrayList<IntWritable>();
    	for (IntWritable i :list) {
    		list2.add(i);
    	}
    	System.out.println(list2);
    	
    
    	
    }
}
