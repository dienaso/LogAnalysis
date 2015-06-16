package com.epweike.la;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.epweike.la.utils.ParserUtil;

public class MapClass extends Mapper<LongWritable, Text, LongWritable, Text> {

	ParserUtil parser = new ParserUtil();
	Text text = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//过滤附件和通过HEAD访问的日志消息
		if(value.toString().contains(("GET /data")) || value.toString().contains(("HEAD"))){
			return;
		}
		
		//解析访问日志行
		final String[] parsed = parser.parse(value.toString());
		
		//过掉开头的特定格式字符串
		if (parsed[2].startsWith("GET/")) {
			parsed[2] = parsed[2].substring("GET/".length());
		} else if (parsed[2].startsWith("POST/")) {
			parsed[2] = parsed[2].substring("POST/".length());
		}
		//过滤结尾的特定格式字符串
		if (parsed[2].endsWith(" HTTP/1.1")) {
			parsed[2] = parsed[2].substring(0,
					parsed[2].length() - " HTTP/1.1".length());
		} else if (parsed[2].endsWith(" HTTP/1.0")) {
			parsed[2] = parsed[2].substring(0,
					parsed[2].length() - " HTTP/1.0".length());
		}

		text.set(parsed[0] + "\t" + parsed[1] + "\t" + parsed[2] + "\t"
				+ parsed[3] + "\t" + parsed[4] + "\t" + parsed[5] + "\t"
				+ parsed[6] + "\t" + parsed[7]);
		context.write(key, text);
	}

}