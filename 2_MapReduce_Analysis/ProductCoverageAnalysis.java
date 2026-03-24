import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProductCoverageAnalysis {

    // ---------------------------------------------------------
    // 1. Weblog Mapper: Subquery A (Web Products)
    // ---------------------------------------------------------
    public static class WeblogMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim().replace("\"", "");
            String[] parts = line.split(",");

            // Weblog schema: ip, datetime, dept, category, product, pageType, status
            if (parts.length < 7) return;

            // INDEX 4 is the Product Name in weblog.csv
            String product = parts[4].trim().toLowerCase();

            if (!product.isEmpty()) {
                context.write(new Text(product), new Text("W"));
            }
        }
    }

    // ---------------------------------------------------------
    // 2. Products Mapper: Subquery B (Store Products Database)
    // ---------------------------------------------------------
    public static class ProductsMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim().replace("\"", "");
            String[] parts = line.split(",");

            // Assuming products.csv has at least 3 columns and product name is at index 2
            if (parts.length < 3) return;

            String product = parts[2].trim().toLowerCase();
            if (!product.isEmpty()) {
                context.write(new Text(product), new Text("S"));
            }
        }
    }

    // ---------------------------------------------------------
    // 3. Reducer: Global Aggregation & Single Row Output
    // ---------------------------------------------------------
    public static class CoverageReducer extends Reducer<Text, Text, Text, NullWritable> {

        // Global counters for this reducer
        private long bothCount = 0;
        private long webOnlyCount = 0;
        private long storeOnlyCount = 0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean hasWeb = false;
            boolean hasStore = false;

            for (Text val : values) {
                String tag = val.toString();
                if (tag.equals("W")) hasWeb = true;
                if (tag.equals("S")) hasStore = true;
                if (hasWeb && hasStore) break; // Optimization
            }

            // Silently increment the counters, DO NOT output anything yet!
            if (hasWeb && hasStore) {
                bothCount++;
            } else if (hasWeb) {
                webOnlyCount++;
            } else {
                storeOnlyCount++;
            }
        }

        // The cleanup method fires exactly ONCE at the very end of the Reducer
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            long totalWebProducts = bothCount + webOnlyCount;
            long totalDbProducts = bothCount + storeOnlyCount;

            double coveragePct = 0.0;
            if (totalDbProducts > 0) {
                coveragePct = (bothCount * 100.0) / totalDbProducts;
            }

            // Print the CSV Header
            String header = "both,web_only,store_only,total_web_products,total_db_products,catalog_coverage_pct";
            context.write(new Text(header), null);

            // Print the single summary row formatting the double to 2 decimal places
            String summaryRow = String.format("%d,%d,%d,%d,%d,%.2f",
                    bothCount, webOnlyCount, storeOnlyCount, totalWebProducts, totalDbProducts, coveragePct);
            context.write(new Text(summaryRow), null);
        }
    }

    // ---------------------------------------------------------
    // 4. Driver
    // ---------------------------------------------------------
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ProductCoverageAnalysis <weblog> <products> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ""); // Keep CSV clean

        Job job = Job.getInstance(conf, "Product Coverage Analysis");
        job.setJarByClass(ProductCoverageAnalysis.class);

        // CRITICAL: Force exactly 1 reducer so our global counters work correctly
        job.setNumReduceTasks(1);

        job.setReducerClass(CoverageReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, WeblogMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ProductsMapper.class);

        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}