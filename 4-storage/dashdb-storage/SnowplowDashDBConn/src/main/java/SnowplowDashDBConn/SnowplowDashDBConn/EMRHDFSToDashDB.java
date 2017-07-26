package SnowplowDashDBConn.SnowplowDashDBConn;

import java.io.IOException;
import java.text.DecimalFormat;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;

import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;

/**
 * Facilitate the push of part files from an EMR instance's temporary HDFS into
 * a DashDB table. From the temporary HDFS, it takes the part files as
 * InputStream's, attaches them to a multi-part DashDB POST request (for data
 * load into a specified table). Measurement of time is taken to figure out
 * efficiency in single-thread operation.
 * 
 * @author StevenHu
 *
 */
public class EMRHDFSToDashDB {

	@Option(name = "--dbuser", usage = "dashdb_user")
	private String dashdb_user;

	@Option(name = "--dbpassword", usage = "dashdb_password")
	private String dashdb_password;

	@Option(name = "--dbhost", usage = "dashdb_host")
	private String dashdb_host;

	@Option(name = "--dbtable", usage = "dashdb_table")
	private String dashdb_table;

	@Option(name = "--hdfspath", usage = "hdfs_path")
	private String hdfs_path;

	public static void main(String[] args) throws InterruptedException {
		new EMRHDFSToDashDB().doMain(args);
	}
	
	public void doMain(String[] args) throws InterruptedException {
        CmdLineParser parser = new CmdLineParser(this);

		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			System.out.println("Please double check the command line params (--dbuser, --dbpassword, "
					+ "--dbhost, --dbtable, --hdfspath).");
			System.exit(12);
		}
		
		System.out.println("DashDB User: " + dashdb_user);
		System.out.println("DashDB Password: " + dashdb_password);
		System.out.println("DashDB Host: " + dashdb_host);
		System.out.println("DashDB Table: " + dashdb_table);
		System.out.println("HDFS Path: " + hdfs_path);
		
		// start time of main running
		long startTime = System.nanoTime(); 
		DecimalFormat df = new DecimalFormat("#.0000");

		// Accept all SSL certificates
		RestAssured.useRelaxedHTTPSValidation();
		RestAssured.authentication = RestAssured.preemptive().basic(dashdb_user, dashdb_password);
		// RequestSpecification is the holder of specifications for a REST call
		RequestSpecification post = RestAssured.given();
		RequestSpecification get = RestAssured.given();

		Gson gson = new Gson();

		Path path = new Path("/" + removeSlashAndHDFS(hdfs_path));
		Configuration conf = new Configuration();

		// Using a try/catch clause to let the JVM close all InputStream's automatically after POST call
		try {
			// get part-files names in array
			FileStatus[] partFiles = getFilePathsInHDFS(path, conf); 
			FileSystem fs = FileSystem.get(conf);

			for (FileStatus file : partFiles) {
				Path filePath = file.getPath();
				String pathString = filePath.toString();
				// This string will be used as the unique request multipart ID and will be used as the 
				// referenced file name
				String file_name = pathString.substring(pathString.lastIndexOf('/') + 1, pathString.length());

				FSDataInputStream fis = null;

				try {
					fis = fs.open(filePath);
				} catch (IOException e) {
					e.printStackTrace();
					System.err.println("Can't open file: " + filePath + ", please check if file is in existence.");
					System.exit(11);
				}

				post.multiPart(file_name, file_name, fis);

				// can incorporate into log instead
				System.out.println("Read and attached " + file_name + " into POST request.");
			}

			// Pushing out POST call and saving the return body
			String postReponse = post
					.post("https://" + dashdb_host + ":8443/dashdb-api/load/local/del/" + dashdb_table + "?delimiter=0x09")
					.asString();

			// Exit connector if DashDB authentication fails
			if (postReponse.contains("Error 401")) {
				System.err.println("\nDashDB login credentials are incorrect, please check username and password "
						+ "for accuracy.");
				System.exit(8);
			}

			ResponseBody postReponseBody = gson.fromJson(postReponse, ResponseBody.class);
			String log = postReponseBody.result.LOAD_LOGFILE.substring(
					postReponseBody.result.LOAD_LOGFILE.indexOf('/') + 1, postReponseBody.result.LOAD_LOGFILE.length());
			System.out.println("\nFinished DashDB POST upload request. Returned Log: " + log + " \n");

			String loadStatus = "";
			ResponseBody getReponseBody = null;

			// while loop to see if POST call is complete by checking "LOAD_STATUS" in GET /loadlogs/logs...
			while (!loadStatus.equals("COMPLETE")) {
				Thread.sleep(30000); // giving 30 seconds of wait time
				String getReponse = get.get("https://" + dashdb_host + ":8443/dashdb-api/home/loadlogs/" + log).asString();
				getReponseBody = gson.fromJson(getReponse, ResponseBody.class);
				loadStatus = getReponseBody.result.LOAD_STATUS;
			}

			// check if input DashDB table exists
			if (getReponseBody.result.LOAD_OUTPUT[0].SQLCODE.contains("SQL3304N")) {
				System.err.println("\nDashDB table <TABLE NAME> does not exist. "
						+ "Please double check table name or create new table, '<TABLE NAME>'");
				System.exit(7);
			}

			System.out.println("Rows Committed: " + getReponseBody.result.ROWS_COMMITTED);
			System.out.println("Rows Deleted: " + getReponseBody.result.ROWS_DELETED);
			System.out.println("Rows Skipped: " + getReponseBody.result.ROWS_SKIPPED);
			System.out
					.println(
							"Success Percentage: "
									+ df.format((1 - ((Double.parseDouble(getReponseBody.result.ROWS_SKIPPED)
											+ Double.parseDouble(getReponseBody.result.ROWS_DELETED))
											/ Double.parseDouble(getReponseBody.result.ROWS_COMMITTED))) * 100)
									+ "%");

		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Path location, " + path.toUri() + " is giving a problem--please recheck for accuracy.");
			System.exit(10);
		}

		// elapsed time print out
		System.out.println("Total Run Time: " + df.format((System.nanoTime() - startTime) / 1000000000.0) + " seconds");
	}

	/**
	 * Given a HDFS folder path, return a list of an array of all the file
	 * details.
	 * 
	 * @param location
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public static FileStatus[] getFilePathsInHDFS(Path location, Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		FileStatus[] items = fileSystem.listStatus(location);

		System.out.println("Number of copied part files into temporary hdfs: " + items.length);

		return items;
	}

	/**
	 * Given a string 's', remove any lead or tail backslashes and the 'hdfs://'
	 * from the beginning of the string
	 * 
	 * @param s
	 * @return
	 */
	public static String removeSlashAndHDFS(String s) {
		System.out.println("Starting Temporary HDFS Location: " + s);
		s = s.startsWith("hdfs://") ? s.substring(7, s.length()) : s;
		s = s.charAt(0) == '/' ? s.substring(1, s.length()) : s;
		s = s.charAt(s.length() - 1) == '/' ? s.substring(0, s.length() - 1) : s;
		System.out.println("Modified Temporary HDFS Location: " + s);
		
		return s;
	}

	public class ResponseBody {
		public Result result;

		public class Result {
			public String ROWS_COMMITTED;
			public String ROWS_DELETED;
			public String ROWS_SKIPPED;
			public String LOAD_LOGFILE;
			public SQLMessage[] LOAD_OUTPUT;
			public String LOAD_STATUS;
		}

		public class SQLMessage {
			public String MESSAGE;
			public String SQLCODE;
		}
	}
}
