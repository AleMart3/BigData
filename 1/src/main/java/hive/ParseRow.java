package hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;



public class ParseRow extends UDF{
 	public Text evaluate(Text text) {
 	
	String[] line = text.toString().split("[,]");
	
	return new Text(line[0] + line[line.length - 2 ]);

	
 	
	 }
}


