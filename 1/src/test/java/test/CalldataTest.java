package test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class CalldataTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public CalldataTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( CalldataTest.class );
    }

    /**
     * Rigourous Test :-)
     * @throws ParseException 
     */
    public void testApp() throws ParseException
    {
        
    	   	
    	
    	String input = "111115|222|0|1|0";
    	String[] output= input.split("|");
    	for(String i : output)
    		System.out.println(i);

    	String inizio= "2015-03-01 09:08:10";
    	String fine = "2015-03-01 10:12:15";
    	SimpleDateFormat data= new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
    	Date d= data.parse(inizio);
    	Date d2= data.parse(fine);
    	Long dminuti = d.getTime();
    	Long d2minuti= d2.getTime();
    	System.out.println(d2minuti/(1000*60)-dminuti/(1000*60));
    
    	
    	/*String data= "01012000";
    	
    	System.out.println(data.substring(2, 4));*/
    
    	
    }
}
