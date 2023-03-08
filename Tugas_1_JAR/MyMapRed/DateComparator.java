package MyMapRed;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateComparator extends WritableComparator {
    protected DateComparator() {
        super(DateSocMedPair.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DateSocMedPair tp1 = (DateSocMedPair) w1;
        DateSocMedPair tp2 = (DateSocMedPair) w2;
        int result = tp1.getDate().compareTo(tp2.getDate());
        if (result == 0) {
            System.out.println("Date failed to be compared");
            result = tp1.getSocMed().compareTo(tp2.getSocMed());
        }
        return result;
    }
}