package job3;
import java.io.IOException;
import java.util.ArrayList;

import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdReducer extends Reducer<Text, ThirdMapperCustomValues, Text, Text> {

	public void reduce(Text key, Iterable<ThirdMapperCustomValues> values, Context context) throws IOException, InterruptedException {

		List<ThirdMapperCustomValues> list = iterableToCollection(values);
		List<ThirdMapperCustomValues> list2 = iterableToCollection(values);

		String[] valori = key.toString().split("[,]");
		String a2016 = valori[0];
		String a2017 = valori[1];
		String a2018 = valori[2]; 


		for (int i = 0; i <list.size(); i++) {
			int a = 1; 
			for (int j = a; i <list2.size(); j++) {
				if (list.get(i)!=list2.get(j) && list.get(i).getSettore()!=list2.get(j).getSettore()
						&& list.get(i).getNome()!=list2.get(j).getNome()){
					context.write(new Text("Anno 2016: " + a2016 + "%, Anno 2017: " + a2017 + "%, Anno 2018: "+ a2018+ "%"), 
							new Text(list.get(i)+ ", " + list2.get(j)));
				}
			}
			a++; 
		}
		//context.write(new Text(key.getNome() + ", " + key.getSettore()), new Text(","+key.getAnno() +", "+  diff_percent));

	}


	public <T> List<T> iterableToCollection(Iterable<T> iterable)
	{
		List<T> collection = new ArrayList<T>();
		for (T e : iterable) {
			collection.add(e);
		}
		return collection;
	}


}

