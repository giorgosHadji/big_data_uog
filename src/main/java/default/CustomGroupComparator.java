package customKey_customPartitioner;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomGroupComparator extends WritableComparator {
    public CustomGroupComparator() {
        super(CustomKey.class, true);
    }
    @Override
    //This class will be used in the reducer in order to group the key and their corresponding values,in order to
    //create the list of values for each key.
    //As we have a composite key but we want to group only by term we have to implement
    //our custom group comparator, grouping only based on the term (called key in the class customKey.)
    public int compare(WritableComparable tp1, WritableComparable tp2) {
    	CustomKey pair1 = (CustomKey) tp1;
    	CustomKey pair2 = (CustomKey) tp2;
        return pair1.key.compareTo(pair2.key);
    }
}
