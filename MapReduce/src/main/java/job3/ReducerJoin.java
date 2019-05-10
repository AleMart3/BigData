package job3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoin extends Reducer <Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String nome_settore="";
		boolean trovato=false;
		List<String> campi_list = new ArrayList<String>();
		for (Text t : values) { 
			String line[] = t.toString().split("\t");
			if (line[0].equals("firstTable")) {
				nome_settore = line[1];
				trovato=true;
			}
			else {
				campi_list.add(line[1]);
			}
		}
		if(trovato) {
			for (String campi : campi_list) { 
				context.write(new Text(key), new Text(","+nome_settore+","+campi));
			}
		}
	}

}


