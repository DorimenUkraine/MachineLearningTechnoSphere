import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SeoPair implements WritableComparable<SeoPair> {
    private Text url;
    private Text query;

    public SeoPair() {
        set("", "");
    }

    public SeoPair(String url, String query) {
        set(url, query);
    }

    private void set(String url, String query) {
        this.url = new Text(url);
        this.query = new Text(query);
    }

    @Override
    public int compareTo(@Nonnull SeoPair o) {
        int cmp = url.toString().compareTo(o.getUrl().toString());

        return cmp == 0 ? query.toString().compareTo(o.getQuery().toString()) : cmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        url.write(out);
        query.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        url.readFields(in);
        query.readFields(in);
    }

    @Override
    public int hashCode() {
        return url.hashCode() * 163 + query.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SeoPair) {
            SeoPair p = (SeoPair) obj;
            return url.equals(p.url) && query.equals(p.query);
        }
        return false;
    }

    @Override
    public String toString() {
        return url + "\t" + query;
    }

    public Text getUrl() {
        return url;
    }

    public void setUrl(Text url) {
        this.url = url;
    }

    public Text getQuery() {
        return query;
    }

    public void setQuery(Text query) {
        this.query = query;
    }
}
