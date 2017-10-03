import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GuessULike {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {

        private final static IntWritable one = new IntWritable(1);
        private HashMap<Text,MapWritable> hashMap;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(),",");
            hashMap = new HashMap();
            while (itr.hasMoreTokens()) {
                hashMap.put(new Text(itr.nextToken()),new MapWritable());
            }
            for (HashMap.Entry<Text,MapWritable> entry: hashMap.entrySet()) {
                addToAnother(entry.getKey());
            }
            for (HashMap.Entry<Text,MapWritable> entry: hashMap.entrySet()) {
                context.write(entry.getKey(),entry.getValue());
            }
        }
        private void addToAnother(Text t){
            for (HashMap.Entry<Text,MapWritable> entry: hashMap.entrySet()) {
                if (!entry.getKey().equals(t)){
                    MapWritable m = entry.getValue();
                    m.put(new Text(t.toString()),one);
                }
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
    public static class Combiner extends Reducer<Text, MapWritable, Text, Text> {}

    public static class IntSumReducer extends Reducer<Text, MapWritable, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        public void reduce(Text key, Iterable<MapWritable> values,Context context) throws IOException, InterruptedException {
            MapWritable results = new MapWritable();
            for (MapWritable val : values) {
                results = putAll(results,val);
            }

            List<MapWritable.Entry> list=new ArrayList<MapWritable.Entry>(results.entrySet());
            Collections.sort(list, new Comparator<MapWritable.Entry>(){

                        public int compare(Map.Entry entry, Map.Entry t1) {
                            return ((IntWritable)t1.getValue()).get()-((IntWritable)entry.getValue()).get();
                        }
                    });

            context.write(key, new Text(StringUtils.join(list.toArray(),',')));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }

        public static MapWritable putAll(MapWritable target, MapWritable plus) {
            Object[] os = plus.keySet().toArray();
            Text key;
            for (int i=0; i<os.length; i++) {
                key = (Text)os[i];
                if (target.containsKey(key))
                    target.put(key, new IntWritable(((IntWritable)target.get(key)).get() + ((IntWritable)plus.get(key)).get()));
                else
                    target.put(key, new IntWritable(((IntWritable)plus.get(key)).get()));
            }
            return target;
        }

    }


    public static void main(String[] args) throws Exception {
        File dir = new File(args[1]);
        judeDirExists(dir);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Item Recommend.");
        job.setJarByClass(GuessULike.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    //Jude the directory output is exist or not, if exist del it
    public static void judeDirExists(File dir) {

        if (dir.exists()) {
            System.out.println("dir exists");
            delFolder(dir.getPath());
        } else {
            System.out.println("dir not exists");

        }
    }
    public static boolean delAllFile(String path) {
        boolean flag = false;
        File file = new File(path);
        if (!file.exists()) {
            return flag;
        }
        if (!file.isDirectory()) {
            return flag;
        }
        String[] tempList = file.list();
        File temp = null;
        for (int i = 0; i < tempList.length; i++) {
            if (path.endsWith(File.separator)) {
                temp = new File(path + tempList[i]);
            } else {
                temp = new File(path + File.separator + tempList[i]);
            }
            if (temp.isFile()) {
                temp.delete();
            }
            if (temp.isDirectory()) {
                delAllFile(path + "/" + tempList[i]);
                delFolder(path + "/" + tempList[i]);
                flag = true;
            }
        }
        return flag;
    }
    public static void delFolder(String folderPath) {
        try {
            delAllFile(folderPath);
            String filePath = folderPath;
            filePath = filePath.toString();
            java.io.File myFilePath = new java.io.File(filePath);
            myFilePath.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

