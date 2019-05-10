package hive;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class String2Date extends UDF{
 	public Text evaluate(Text text) {
 	if(text == null) return null;
 	String data = text.toString();
 	DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	LocalDate date = LocalDate.parse(data, format);
	return new Text(String.valueOf(date.getYear()));
 	
	 }
}


