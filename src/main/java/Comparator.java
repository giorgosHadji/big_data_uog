import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
 
public class Comparator extends WritableComparator {
 
    public Comparator() {
        super(CustomKey.class, true);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    //in this class we implement the method compare which will be used in the sorting keys process.
    //By using a composite key, we can sort also by term frequency as a secondary sort.
    //frequency must be sorted in descending order.
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        
    	CustomKey key1 = (CustomKey) wc1;
    	CustomKey key2 = (CustomKey) wc2;
        
    	int stateCmp = key1.key.compareTo(key2.key);
		if (stateCmp != 0) {
			return stateCmp;
		} else {
			return - key1.freq.compareTo(key2.freq);
		}
    }
    
 
}