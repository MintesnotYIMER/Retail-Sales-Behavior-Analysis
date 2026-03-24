package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogToCSV {

    public static class LogMapper extends Mapper<Object, Text, NullWritable, Text> {

        static final Pattern P_FULL = Pattern.compile(
                "([^ ]*) - - \\[([^]]*)] \"[^ ]* " +
                        "/department/([^/]*)/category/([^/]*)/product/([^ ]*) HTTP[^ ]* (\\d{3})"
        );
        static final Pattern P_CAT = Pattern.compile(
                "([^ ]*) - - \\[([^]]*)] \"[^ ]* " +
                        "/department/([^/]*)/category/([^ ]*) HTTP[^ ]* (\\d{3})"
        );
        static final Pattern P_DEPT = Pattern.compile(
                "([^ ]*) - - \\[([^]]*)] \"[^ ]* " +
                        "/department/([^ ]*) HTTP[^ ]* (\\d{3})"
        );
        static final Pattern P_PAGE = Pattern.compile(
                "([^ ]*) - - \\[([^]]*)] \"[^ ]* " +
                        "/([^ /]*) HTTP[^ ]* (\\d{3})"
        );

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            Matcher m;

            // Pattern 1: Full product URL (/department/X/category/X/product/X)
            m = P_FULL.matcher(line);
            if (m.find()) {
                String ip       = m.group(1);
                String[] dt     = splitDatetime(m.group(2));
                String dept     = decode(m.group(3));
                String cat      = decode(m.group(4));
                String prodRaw  = decode(m.group(5));
                String respCode = m.group(6);

                if (!respCode.equals("200")) return;

                //  CHANGE 1: removed boolean cart, addToCart no longer passed
                boolean cart    = prodRaw.endsWith("/add_to_cart");
                String product  = cart ? prodRaw.replace("/add_to_cart", "") : prodRaw;
                String pageType = cart ? "add_to_cart" : "product";

                emit(context, ip, dt[0], dt[1], dept, cat, product, pageType, respCode);
                return;
            }

            // Pattern 2: Category URL (/department/X/category/X)
            m = P_CAT.matcher(line);
            if (m.find()) {
                String respCode = m.group(5);
                if (!respCode.equals("200")) return;

                String[] dt = splitDatetime(m.group(2));
                // CHANGE 2: removed "false" addToCart argument
                emit(context, m.group(1), dt[0], dt[1],
                        decode(m.group(3)), decode(m.group(4)),
                        "", "category", respCode);
                return;
            }

            // Pattern 3: Department URL (/department/X)
            m = P_DEPT.matcher(line);
            if (m.find()) {
                String respCode = m.group(4);
                if (!respCode.equals("200")) return;

                String[] dt = splitDatetime(m.group(2));
                //  CHANGE 3: removed "false" addToCart argument
                emit(context, m.group(1), dt[0], dt[1],
                        decode(m.group(3)), "", "", "department", respCode);
                return;
            }

            // Pattern 4: Other pages (/checkout, /home, /login, etc.)
            m = P_PAGE.matcher(line);
            if (m.find()) {
                String respCode = m.group(4);
                if (!respCode.equals("200")) return;

                String[] dt = splitDatetime(m.group(2));
                //  CHANGE 4: removed "false" addToCart argument
                emit(context, m.group(1), dt[0], dt[1],
                        "", "", "", m.group(3), respCode);
            }
        }

        private String[] splitDatetime(String raw) {
            String[] parts = raw.split(":");
            return new String[]{ parts[0], parts[1] + ":" + parts[2] + ":" + parts[3].split(" ")[0] };
        }

        private String decode(String s) {
            try { return URLDecoder.decode(s, "UTF-8"); }
            catch (Exception e) { return s; }
        }

        //  CHANGE 5: removed addToCart parameter from signature and format string
        private void emit(Context ctx, String ip, String date, String time,
                          String dept, String cat, String product,
                          String pageType, String respCode)
                throws IOException, InterruptedException {

            String datetime = date + " " + time;
            String row = String.format("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",%s",
                    ip, datetime, dept, cat, product, pageType, respCode);
            ctx.write(NullWritable.get(), new Text(row));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log to CSV");
        job.setJarByClass(LogToCSV.class);
        job.setMapperClass(LogMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
