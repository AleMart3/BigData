package topN1mapreduce;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

       int sum = 0;
       for (IntWritable val : values) {
          sum += val.get();
     }

   
   // We need to create another Text object because the Text instance
   // we receive is the same for all the words
        countMap.put(new Text(key), new IntWritable(sum));
    }
    
    
    //questo metodo viene chiamato dopo che è finita tutta la reduce
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Map<Text, IntWritable> sortedMap = sortByValues(countMap);

        int counter = 0;
        for (Text key : sortedMap.keySet()) {
            if (counter++ == 3) {
                break;
            }
            context.write(key, sortedMap.get(key));
        }
    }
    
    
    /*
     * sorts the map by values. Taken from:
     * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
     */
      @SuppressWarnings("rawtypes")
      private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
    	  
    	  //trasforma la mappa in una lista dove ogni elemento della lista è chiave=valore
    	  
          List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());
          
          Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
        	  @SuppressWarnings("unchecked")
        	  public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
              return o2.getValue().compareTo(o1.getValue());
              }
          });

          //LinkedHashMap will keep the keys in the order they are inserted
          Map<K, V> sortedMap = new LinkedHashMap<K, V>();

          for (Map.Entry<K, V> entry : entries) {
              sortedMap.put(entry.getKey(), entry.getValue());
          }

          return sortedMap;
      }
    
    

    
}