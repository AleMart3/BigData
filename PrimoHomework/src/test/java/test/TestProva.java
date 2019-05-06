package test;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
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
           
    	/*
    	SimpleDateFormat format= new SimpleDateFormat("yyyy-mm-dd");
		Date date = format.parse("1998-02-02");
		System.out.println(date.getYear());
		
		String s= "AHH,11.5,11.5799999237061,8.49315452575684,11.25,11.6800003051758,4633900,2013-05-08";
		String[] line = s.split("[,]");
		String close= line[1];
		Double c = Double.parseDouble(close);
		
		System.out.println(c);
    	//--------------------------------------
    	long start = System.currentTimeMillis();

        long end = System.currentTimeMillis();

    	NumberFormat formatter = new DecimalFormat("#0.00000");
    	System.out.print("Execution time is " + formatter.format((end - start) / 1000d) + " seconds");
    	*/
    /*	final Calendar c = Calendar.getInstance();
    	System.out.println(c.getTime());
        c.set(2004, 10, 02);
    	System.out.println(c.getTimeInMillis());
    	*/
    	//System.out.println(java.time.LocalDate.of(2004, 10, 02).get);
    	
    /*	LocalDate l = LocalDate.of(2004, 12, 2);
    	LocalDate l2 = LocalDate.of(2004, 12, 1);
    	System.out.println(l.toEpochDay());
    	System.out.println(l2.toEpochDay());
    	
    	System.out.println(l.getYear());*/
    	final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    	final LocalDate dt = LocalDate.parse("2000-02-01", formatter);
    	
		System.out.println(dt);
    	
    
    
    	
    	
    }
}
