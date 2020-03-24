package customKey_customPartitioner;

import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<CustomKey,FreqDocPair> {

	@Override
	//As we want our final Index to be sorted by term we need to create a partitioning
	//that will have a total order in the output files
	//A way to do this is by assigning the keys to reducers in alphabetical order.
	//Example a->0,1, b-> 2,3 etc

	public int getPartition(CustomKey key, FreqDocPair value, int numPartitions) {
		int c = Character.toLowerCase(key.key.charAt(0));
		int res=0;
		int mul=1;
		if (c < 'a' || c > 'z')
			return numPartitions - 1;
		if (numPartitions>53) {
			mul=2;
			if(key.key.length()>1) {
				int b = Character.toLowerCase(key.key.charAt(1));

				if (b>'k') {
					res=1;
				}
			}
		}
		return res+mul*(int)Math.floor((float)((numPartitions - 1)/mul) * (c-'a')/('z'-'a'));

	}}
