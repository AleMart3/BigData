package hive;


import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;



public class ParseRow2 extends GenericUDTF {
	 
	  Integer count = Integer.valueOf(0);
	  Object forwardObj[] = new Object[1];
     
	  private PrimitiveObjectInspector stringOI = null;
	 
	
	 
	  @Override
	  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		  
		  stringOI = (PrimitiveObjectInspector) args[0];
		  
	      List<String> fieldNames = new ArrayList<String>(2);
	      List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);

	      fieldNames.add("ticker");
	      fieldNames.add("sector");
	      fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	      fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	      return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	  }
	 
	  @Override
	  public void process(Object[] record) throws HiveException {
	    final String document = (String) stringOI.getPrimitiveJavaObject(record[0]);

	    if (document == null) {
	      return;
	    }
	    String[] line = document.split("[,]");
	    
	      forward(new Object[] { line[0], line[line.length-2]});
	    
	  }
	  
	  
	  

	  @Override
	  public void close() throws HiveException {
	    // do nothing
	  }
}