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
public class TestProva 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public TestProva( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( TestProva.class );
    }

    /**
     * Rigourous Test :-)
     * @throws ParseException 
     */
    @SuppressWarnings("deprecation")
	public void testApp() throws ParseException
    {
           
    	
    	SimpleDateFormat format= new SimpleDateFormat("yyyy-mm-dd");
		Date date = format.parse("1998-02-02");
		System.out.println(date.getYear());
    	
    	
    }
}
