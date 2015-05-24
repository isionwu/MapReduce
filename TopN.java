import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class TopN {

	public static class TopNMapper extends Mapper <Object, Text, Text, IntWritable>{
		final IntWritable one = new IntWritable(1);
		Text word = new Text();
		String token = "[\\pP‘’“”]";
		
		@Override
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
			String cleanLine = value.toString().toLowerCase().replaceAll(token," ");
			StringTokenizer itr = new StringTokenizer(cleanLine);
			while(itr.hasMoreTokens()){
				word.set(itr.nextToken().trim());
				context.write(word, one);
			}
		}
	}
	
	public static class TopNReducer extends Reducer <Text, IntWritable, Text, IntWritable>{
		Map<Text, IntWritable> countMap = new HashMap<>();
		@Override
		public void reduce(Text key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val : value)
				sum += val.get();
			countMap.put(new Text(key), new IntWritable(sum));
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			Map<Text, IntWritable> sortedMap = sortedByValues(countMap);
			
            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 20) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
		}
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public static <K extends Comparable, V extends Comparable> Map<K,V> sortedByValues(Map<K,V> map){
			List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K,V>>(map.entrySet());
			Collections.sort(entries,new Comparator<Map.Entry<K, V>>(){
				@Override
				public int compare(Map.Entry<K, V> o1, Map.Entry<K,V> o2){
					return o2.getValue().compareTo(o1.getValue());
				}
			});
			
			Map<K,V> sortedMap = new LinkedHashMap<K,V>();
			for(Map.Entry<K, V> entry : entries){
				sortedMap.put(entry.getKey(), entry.getValue());
			}
			return sortedMap;
		}
	}
}
