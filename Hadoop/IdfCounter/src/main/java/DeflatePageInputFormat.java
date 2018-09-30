import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class DeflatePageInputFormat extends FileInputFormat<LongWritable, Text> {

    public class PageRecordReader extends org.apache.hadoop.mapreduce.RecordReader<LongWritable, Text> {
        private FSDataInputStream input;
        private FSDataInputStream inputIdx;

        private byte[] buf = new byte[4];
        private byte[] uncompressed = new byte[4096];

        private long nRecords = 0;
        private long curRecord = 0;

        private long key = 0;
        private Text text = new Text("");

        @Override
        public void initialize(org.apache.hadoop.mapreduce.InputSplit split, org.apache.hadoop.mapreduce.TaskAttemptContext context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            FileSplit fsplit = (FileSplit) split;

            Path idxPath = fsplit.getPath();
            String idxUrl = idxPath.toString();
            String srcUrl = idxUrl.substring(0, idxUrl.length() - 4);

            Path path = new Path(srcUrl);
            FileSystem fs = path.getFileSystem(conf);
            FileSystem idxFs = idxPath.getFileSystem(conf);

            this.nRecords = fsplit.getLength() / 4;

            input = fs.open(path);
            inputIdx = idxFs.open(idxPath);

            int idxShift = 0;
            int srcShift = 0;

            while (idxShift < fsplit.getStart()) {
                inputIdx.readFully(buf, 0, 4);

                int bytes2read = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt();
                srcShift += bytes2read;
                idxShift += 4;
            }

            key = srcShift;
            input.seek(srcShift);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            Inflater inflater = new Inflater();

            if (curRecord >= nRecords) {
                return false;
            }

            inputIdx.readFully(buf, 0, 4);
            int bytes2read = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt();

            byte[] buf2 = new byte[bytes2read];
            input.readFully(buf2, 0, bytes2read);

            inflater.setInput(buf2);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            while (!inflater.finished()) {
                int len = 0;
                try {
                    len = inflater.inflate(uncompressed, 0, 4096);
                } catch (DataFormatException e) {
                     throw new IOException("Problem with inflater: " + String.valueOf(curRecord) + " " + String.valueOf(key));
                }

                bos.write(uncompressed, 0, len);
            }
            bos.close();

            text = new Text(bos.toString(StandardCharsets.UTF_8.name()));
            curRecord++;
            key += bytes2read;

            return true;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return new LongWritable(this.key);
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return this.text;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return ((float) this.curRecord + 1) / this.nRecords;
        }

        @Override
        public void close() throws IOException {
             IOUtils.closeStream(input);
             IOUtils.closeStream(inputIdx);
        }
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        PageRecordReader reader = new PageRecordReader();
        reader.initialize(split, context);

        return reader;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        List<InputSplit> splits = new ArrayList<>();

        for (FileStatus status: listStatus(job)) {
            Path idxPath = new Path(status.getPath().toString() + ".idx");
            FileSystem idxFs = idxPath.getFileSystem(job.getConfiguration());
            long len = idxFs.getFileStatus(idxPath).getLen();

            int start = 0;
            long per_split = job.getConfiguration().getLong(BYTES_PER_MAP, 12000);

            while (len > 0) {
                if (len < per_split) {
                    splits.add(new FileSplit(idxPath, start, len, null));
                    break;
                } else {
                    splits.add(new FileSplit(idxPath, start, per_split, null));
                    len -= per_split;
                    start += per_split;
                }
            }
        }

        return splits;
    }

    public String BYTES_PER_MAP = "mapreduce.input.indexedgz.bytespermap";
}
