package salaryAnalyzer;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SalaryAnalyzer2 {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(SalaryAnalyzer2.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2014004");                   // ★自分の学籍番号

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 入出力ファイルを指定
		String inputpath = "out/salary1";
		String outputpath = "out/salary2";     // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(8);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);
	}


	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static final int TYPE = 0;
		private static final int COUNT = 1;

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split("\t");

			// 分類を取得
			String type = csv[TYPE];

			// 売り上げを取得
			String count = csv[COUNT];

			//売り上げ0は無視
			if(count.equals("0")) return;

			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new Text(type), new Text(count));
		}

	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// 同じ売り上げがあれば合わせる
			long count = 0;
			long totalCount = 0;
			for (Text value : values) {
				count++;
				totalCount += Long.valueOf(value.toString());
			}

			// emit
			context.write(new Text(format(Integer.valueOf(key.toString()))),new Text(count+"\t"+totalCount));
		}
	}

	private static String format(int key){
		switch(key){
		case 1:
			return "100円以下";
		case 2:
			return "200円以下";
		case 3:
			return "300円以下";
		case 4:
			return "400円以下";
		case 5:
			return "500円以下";
		case 6:
			return "600円以下";
		case 7:
			return "700円以下";
		case 8:
			return "800円以下";
		case 9:
			return "900円以下";
		case 10:
			return "1000円以下";
		case 11:
			return "1500円以下";
		case 12:
			return "2000円以下";
		case 13:
			return "5000円以下";
		case 14:
			return "10000円以下";
		case 15:
			return "10000円以上";
		default:
			return "";
		}
	}

}

