import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class CategoryPresenceAnalysis {

    // Regex to split comma-separated values ignoring commas inside quotes (Mimics Spark!)
    private static final String CSV_REGEX = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    // Helper method to remove quotes from parsed CSV strings
    private static String cleanField(String field) {
        return field.replaceAll("^\"|\"$", "").trim();
    }

    // =========================================================================
    // MAPPER 1: Weblog Mapper
    // =========================================================================
    public static class WeblogMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text("W");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(CSV_REGEX);
            if (fields.length >= 6) {
                String categoryRaw = cleanField(fields[3]);
                String pageType = cleanField(fields[5]).toLowerCase();

                if ((pageType.equals("add_to_cart") || pageType.equals("product") || pageType.equals("category"))
                        && !categoryRaw.isEmpty()) {

                    String cleanCategory = categoryRaw;
                    if (cleanCategory.contains("/product")) cleanCategory = cleanCategory.substring(0, cleanCategory.indexOf("/product"));
                    else if (cleanCategory.contains("/add_to_cart")) cleanCategory = cleanCategory.substring(0, cleanCategory.indexOf("/add_to_cart"));

                    outKey.set(cleanCategory.toLowerCase());
                    context.write(outKey, outValue);
                }
            }
        }
    }

    // =========================================================================
    // MAPPER 2: Store Order Mapper
    // =========================================================================
    public static class StoreOrderMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text("S");
        private Map<String, String> productIdToCatId = new HashMap<>();
        private Map<String, String> catIdToCatName = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                FileSystem fs = FileSystem.get(context.getConfiguration());

                // Read cache dynamically by checking the file name so order doesn't matter
                for (URI uri : cacheFiles) {
                    Path path = new Path(uri);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                    String line;

                    if (path.getName().contains("products")) {
                        while ((line = reader.readLine()) != null) {
                            String[] pFields = line.split(CSV_REGEX);
                            if (pFields.length >= 3) productIdToCatId.put(cleanField(pFields[0]), cleanField(pFields[1]));
                        }
                    } else if (path.getName().contains("categories")) {
                        while ((line = reader.readLine()) != null) {
                            String[] cFields = line.split(CSV_REGEX);
                            if (cFields.length >= 3) catIdToCatName.put(cleanField(cFields[0]), cleanField(cFields[2]).toLowerCase());
                        }
                    }
                    reader.close();
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(CSV_REGEX);
            if (fields.length >= 3) {
                String productId = cleanField(fields[2]);
                String catId = productIdToCatId.get(productId);
                if (catId != null) {
                    String catName = catIdToCatName.get(catId);
                    if (catName != null) {
                        outKey.set(catName);
                        context.write(outKey, outValue);
                    }
                }
            }
        }
    }

    // =========================================================================
    // REDUCER (Using NullWritable to eliminate the Tab character completely!)
    // =========================================================================
    public static class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text outValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean inWeb = false;
            boolean inStore = false;

            for (Text val : values) {
                if (val.toString().equals("W")) inWeb = true;
                if (val.toString().equals("S")) inStore = true;
            }

            String category = capitalize(key.toString());

            // If the category has a comma in it naturally, wrap it in quotes for safety
            if (category.contains(",")) category = "\"" + category + "\"";

            String presence = "";
            if (inWeb && inStore) presence = "Both (Web & Store)";
            else if (inWeb) presence = "Web Only";
            else presence = "Store Only";

            // Format as a single perfect string and output with no key
            outValue.set(category + "," + presence);
            context.write(NullWritable.get(), outValue);
        }

        private String capitalize(String text) {
            String[] words = text.split("\\s+"); // Handles double spaces
            StringBuilder sb = new StringBuilder();
            for (String word : words) {
                if (word.length() > 0) sb.append(Character.toUpperCase(word.charAt(0))).append(word.substring(1)).append(" ");
            }
            return sb.toString().trim();
        }
    }

    // =========================================================================
    // DRIVER
    // =========================================================================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Category Presence Analysis Final Fixed");
        job.setJarByClass(CategoryPresenceAnalysis.class);

        job.addCacheFile(new URI("/input/products/products.csv"));
        job.addCacheFile(new URI("/input/categories/categories.csv"));

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, WeblogMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, StoreOrderMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setReducerClass(JoinReducer.class);

        // Map Outputs (Keys and Tags)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final Reducer Outputs (No Key, Only Text)
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}