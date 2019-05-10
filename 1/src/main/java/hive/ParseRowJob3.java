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


public class ParseRowJob3 extends GenericUDTF {
	 
	  Integer count = Integer.valueOf(0);
	  Object forwardObj[] = new Object[1];
     
	  private PrimitiveObjectInspector stringOI = null;
	 
	
	 
	  @Override
	  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		  
		  stringOI = (PrimitiveObjectInspector) args[0];
		  
	      List<String> fieldNames = new ArrayList<String>(2);
	      List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);

	      fieldNames.add("ticker");
	      fieldNames.add("name");
	      fieldNames.add("sector");
	      fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
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
	    
	    String name="";
	    String sector="";
	    		
		
		if(line.length == 7 | (line.length == 6 && line[2].contains((" \" ").trim()))){
			 name = (line[2]+ " " + line[3]).trim();
			(name).trim().substring(1, name.length()-2);
			 sector= line[4];
		} else {
			name = line[2];
			sector= line[3];
		}

	    
	      forward(new Object[] { line[0], name, sector});
	    
	  }
	  
	  
	  

	  @Override
	  public void close() throws HiveException {
	    // do nothing
	  }
}