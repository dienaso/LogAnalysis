/**
 * 
 */
package com.epweike.la.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import ua_parser.Client;
import ua_parser.Parser;

public class ParserUtil {
	public static final SimpleDateFormat FORMAT = new SimpleDateFormat(
			"d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
	public static final SimpleDateFormat dateformat = new SimpleDateFormat(
			"yyyyMMddHHmmss");

	public static void main(String[] args) throws ParseException, IOException {
		// final String S1 =
		// "101.226.33.200 - - [24/May/2015:07:53:30 +0800] \"GET /index.php?do=yun&view=login&op=login&session_name=PHPSESSID&session_id=7cb25e00a4fdf94341da34997aebbb4281683153 HTTP/1.1\" 200 5 \"http://www.epweike.com/index.php?do=user&view=index\" \"Mozilla/5.0 (Linux; U; Android 4.4.2; zh-cn; GT-I9500 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko)Version/4.0 MQQBrowser/5.0 QQ-Manager Mobile Safari/537.36\" - \"0.214";
		// ParserUtil parser = new ParserUtil();
		// final String[] array = parser.parse(S1);
		// System.out.println("样例数据： " + S1);
		// System.out
		// .format("解析结果：  remote_addr=%s, time_local=%s, request=%s, status=%s, http_referer=%s, body_bytes_sent=%s, http_user_agent=%s, request_time=%s",
		// array[0], array[1], array[2], array[3], array[4],
		// array[5], array[6], array[7]);
		ParserUtil parserUtil = new ParserUtil();
		parserUtil.readAndPrase();
		// Parser uaParser = new Parser();
		// Client c = uaParser.parse(S1);
		//
		// System.out.println(c.userAgent.family); // => "Mobile Safari"
		// System.out.println(c.userAgent.major); // => "5"
		// System.out.println(c.userAgent.minor); // => "1"
		//
		// System.out.println(c.os.family); // => "iOS"
		// System.out.println(c.os.major); // => "5"
		// System.out.println(c.os.minor); // => "1"
		//
		// System.out.println(c.device.family); // => "iPhone"

	}

	/**
	 * 格式化时间
	 * 
	 * @param string
	 * @return
	 * @throws ParseException
	 */
	private Date parseDateFormat(String string) {
		Date parse = null;
		try {
			parse = FORMAT.parse(string);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return parse;
	}

	public void readAndPrase() {
		String fileName = "C:\\Users\\Administrator\\Desktop\\logs\\brand.epweike.com-access.log-20150525";

		File file = new File(fileName);
		BufferedReader reader = null;
		try {
			System.out.println("以行为单位读取文件内容，一次读一整行：");
			// 计时开始
			long t1 = System.currentTimeMillis();

			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(file), "utf-8"));
			String tempString = null;
			int line = 1;
			// 一次读入一行，直到读入null为文件结束
			while ((tempString = reader.readLine()) != null) {
				String[] parsed = parse(tempString);
				// 打印行号与解析结果
				System.out.println("line " + line + ": " + parsed[6]);
				line++;
			}

			reader.close();

			long t2 = System.currentTimeMillis(); // 排序后取得当前时间

			// 计时结束
			Calendar c = Calendar.getInstance();
			c.setTimeInMillis(t2 - t1);
			System.out.println("耗时: " + c.get(Calendar.MINUTE) + "分 "
					+ c.get(Calendar.SECOND) + "秒 "
					+ c.get(Calendar.MILLISECOND) + " 毫秒");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}

	}

	/**
	 * 解析日志的行记录
	 * 
	 * @param line
	 * @return 数组含有8个元素
	 * @throws IOException
	 */
	public String[] parse(String line) throws IOException {
		String remote_addr = parseRemoteAddr(line);
		String time_local = parseTimeLocal(line);
		String request = parseRequest(line);
		String status = parseStatus(line);
		String http_referer = parseHttpReferer(line);
		String body_bytes_sent = parseBodyBytesSent(line);
		String http_user_agent = parseHttpUserAgent(line);
		String request_time = parseRequestTime(line);

		return new String[] { remote_addr, time_local, request, status,
				body_bytes_sent, http_referer, http_user_agent, request_time };
	}

	/**
	 * @Description:解析整个请求的总时间
	 * 
	 *                         $request_time
	 * @author 吴小平
	 * @version 创建时间：2015年5月26日 下午4:24:10
	 */
	private String parseRequestTime(String line) {
		final int first = line.lastIndexOf("-");
		String request_time = line.substring(first + 3, line.length() - 1);
		return request_time;
	}

	/**
	 * @Description:解析用户终端代理
	 * 
	 *                       $http_user_agent
	 * @author 吴小平
	 * @version 创建时间：2015年5月26日 下午4:24:10
	 * @throws IOException
	 */
	private String parseHttpUserAgent(String line) throws IOException {
		final int end = line.lastIndexOf("-");
		String tmp = line.substring(0, end - 2);
		final int first = tmp.lastIndexOf("\"");
		String http_user_agent = tmp.substring(first + 1).trim();
		// 解析
		Parser uaParser = new Parser();
		Client c = uaParser.parse(http_user_agent);
		http_user_agent = c.userAgent.family + "," + c.os.family + ","
				+ c.device.family;

		return http_user_agent;
	}

	/**
	 * @Description:解析跳转来源
	 * 
	 *                     $http_referer
	 * @author 吴小平
	 * @version 创建时间：2015年5月26日 下午4:24:10
	 */
	private String parseHttpReferer(String line) {
		final int first = line.indexOf("HTTP/");
		String http_referer = line.substring(first + 14);
		http_referer = http_referer.split(" ")[1].replace("\"", "").trim();
		return http_referer;
	}

	/**
	 * @Description:解析发送给客户端文件内容大小
	 * 
	 *                             $body_bytes_sent
	 * @author 吴小平
	 * @version 创建时间：2015年5月26日 下午4:24:10
	 */
	private String parseBodyBytesSent(String line) {
		final int first = line.indexOf("HTTP/");
		String body_bytes_sent = line.substring(first + 14);
		body_bytes_sent = body_bytes_sent.split(" ")[0].trim();
		return body_bytes_sent;
	}

	/**
	 * @Description:HTTP请求状态
	 * 
	 *                       $status
	 * @author 吴小平
	 * @version 创建时间：2015年5月26日 下午4:24:10
	 */
	private String parseStatus(String line) {
		final int first = line.indexOf("HTTP/");
		String status = line.substring(first + 10, first + 13);
		return status;
	}

	/**
	 * @Description:解析请求的URI和HTTP协议
	 * 
	 *                              $request
	 * @author 吴小平
	 * @version 创建时间：2015年5月26日 下午4:24:10
	 */
	private String parseRequest(String line) {
		final int first = line.indexOf("\"");

		String request = line.substring(first + 1);
		request = request.split(" ")[0].trim() + request.split(" ")[1].trim();
		return request;
	}

	/**
	 * @Description:解析访问时间
	 * 
	 *                     $time_local
	 * @author 吴小平
	 * @version 创建时间：2015年5月26日 下午4:24:10
	 */
	private String parseTimeLocal(String line) {
		final int first = line.indexOf("[");
		final int last = line.indexOf("+0800]");
		String time_local = line.substring(first + 1, last).trim();
		Date date = parseDateFormat(time_local);
		return dateformat.format(date);
	}

	/**
	 * @Description:解析客户端地址
	 * 
	 *                      $remote_addr
	 * @author 吴小平
	 * @version 创建时间：2015年5月26日 下午4:24:10
	 */
	private String parseRemoteAddr(String line) {
		String remote_addr = line.split("- -")[0].trim();
		return remote_addr;
	}
}
