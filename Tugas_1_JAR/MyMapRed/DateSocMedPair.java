package MyMapRed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.io.WritableComparable;

public class DateSocMedPair implements WritableComparable<DateSocMedPair> {
    private String date;
    private String socmed;

    public DateSocMedPair() {
        this.date = "";
        this.socmed = "";
    }

    public DateSocMedPair(String date, String socmed){
        try {
            this.date = constructDate(date);
        } catch (ParseException e) {
            System.err.println("failed to parse " + date + " " + e.toString());
            e.printStackTrace();
        }
        this.socmed = socmed;
    }

    public String getDate() {
        return this.date;
    }

    public String getSocMed() {
        return this.socmed;
    }


    // /yyyy-MM-dd format

    public String constructDate(String s) throws ParseException {
        if(StringUtils.isNumeric(s)) {
            long epochVal = Long.parseLong(s);
            Date d = new Date(epochVal * 1000);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            return formatter.format(d);
        } else{
            // String of socmed date format, beside Instagram POSIX time format
            DateFormat[] possibleFormats = {
                    new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy"),
                    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"),
                    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ"),
                    new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
            };

            for (DateFormat format : possibleFormats) {
                try {
                    Date result = format.parse(s);
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                    return formatter.format(result);
                } catch (ParseException e) {
                }
            }
        }
        return s;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.date = in.readUTF();
        this.socmed = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.date);
        out.writeUTF(this.socmed);
    }

    @Override
    public int compareTo(DateSocMedPair other) {
        try {
            SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd");
            Date a = parser.parse(this.date);
            Date b = parser.parse(other.date);

            int res = a.compareTo(b);
            if(res == 0) {
                return this.socmed.compareTo(other.socmed);
            }
            return res;
        } catch(Exception e) {
            System.out.println("Comparing " + date.toString() + " " + other.date + "With error" + e.getMessage());
        }
        return this.date.compareTo(other.date);
    }

    @Override
    public String toString() {
        return this.date + "," + this.socmed;
    }

}