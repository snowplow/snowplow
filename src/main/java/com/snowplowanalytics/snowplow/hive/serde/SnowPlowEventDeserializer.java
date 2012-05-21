/* 
 * Copyright (c) 2012 Orderly Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.hive.serde;

// Java
import java.nio.charset.CharacterCodingException;
import java.util.List;
import java.util.Properties;

// Commons Logging
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Hadoop
import org.apache.hadoop.conf.Configuration;

// Hive
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ReflectionStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * CfLogDeserializer reads CloudFront download distribution file access log data into Hive.
 * 
 * For documentation please see the introductory README.md in the project root.
 */
public class CfLogDeserializer implements Deserializer {

  // -------------------------------------------------------------------------------------------------------------------
  // Initial setup
  // -------------------------------------------------------------------------------------------------------------------

  // Setup logging
  public static final Log LOG = LogFactory.getLog(CfLogDeserializer.class.getName());

  // Voodoo taken from Zemanta's S3LogDeserializer
  static {
    StackTraceElement[] sTrace = new Exception().getStackTrace();
    sTrace[0].getClassName();
  }

  // We'll initialize our object inspector below
  private ObjectInspector cachedObjectInspector;

  // For performance reasons we reuse the same object to deserialize all of our rows
  private static final CfLogStruct cachedStruct = new CfLogStruct();

  // -------------------------------------------------------------------------------------------------------------------
  // Only test - TODO move this out into test suite
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * @param args
   */
  public static void runTest() {
    System.err.println("This is only a test run");
    try {
      CfLogDeserializer serDe = new CfLogDeserializer();
      Configuration conf = new Configuration();
      Properties tbl = new Properties();
      Text sample = new Text("2012-03-16	11:45:01	ARN1	3422	195.78.71.32	GET	detlpfvsg0d9v.cloudfront.net	/ice.png	200	http://delivery.ads-creativesyndicator.com/adserver/www/delivery/afr.php?zoneid=103&cb=INSERT_RANDOM_NUMBER_HERE&ct0=INSERT_CLICKURL_HERE	Mozilla/5.0%20(Windows%20NT%206.0)%20AppleWebKit/535.11%20(KHTML,%20like%20Gecko)%20Chrome/17.0.963.79%20Safari/535.11	&ad_ba=1884&ad_ca=547&ad_us=a1088f76c6931b0a26228dc3bde321d7&r=481413&urlref=http%253A%252F%252Fwww.fantasyfootballscout.co.uk%252F&_id=b41cf6859dccd8ce&_ref=http%253A%252F%252Fwww.fantasyfootballscout.co.uk%252F&pdf=1&qt=0&realp=0&wma=0&dir=1&fla=1&java=1&gears=0&ag=1&res=1920x1200&cookie=1");
      // Text sample = new Text("02/01/2011 01:13:12 LAX1 2390282 192.0.2.202 GET www.singalong.com /soundtrack/happy.mp3 304 www.unknownsingers.com Mozilla/4.0%20(compatible;%20MSIE%207.0;%20Windows%20NT%205.1) a=b&c=d");
      serDe.initialize(conf, tbl);
      Object row = serDe.deserialize(sample);
      System.err.println(serDe.getObjectInspector().getClass().toString());
      ReflectionStructObjectInspector oi = (ReflectionStructObjectInspector) serDe
          .getObjectInspector();
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
      for (int i = 0; i < fieldRefs.size(); i++) {
        System.err.println(fieldRefs.get(i).toString());
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
        if (fieldData == null) {
          System.err.println("null");
        } else {
          System.err.println(fieldData.toString());
        }
      }

    } catch (Exception e) {
      System.err.println("Caught: " + e);
      e.printStackTrace();
    }
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Constructor & initializer
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Empty constructor
   */
  public CfLogDeserializer() throws SerDeException {
  }

  /**
   * Initialize the CfLogDeserializer.
   *
   * @param conf System properties
   * @param tbl Table properties
   * @throws SerDeException For any exception during initialization
   */
  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {

    cachedObjectInspector = ObjectInspectorFactory.getReflectionObjectInspector(CfLogStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    LOG.debug(this.getClass().getName() + " initialized");
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Deserializer
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Deserialize an object out of a Writable blob. In most cases, the return
   * value of this function will be constant since the function will reuse the
   * returned object. If the client wants to keep a copy of the object, the
   * client needs to clone the returned value by calling
   * ObjectInspectorUtils.getStandardObject().
   * 
   * @param blob The Writable object containing a serialized object
   * @return A Java object representing the contents in the blob.
   * @throws SerDeException For any exception during initialization
   */
  @Override
  public Object deserialize(Writable field) throws SerDeException {
    String row = null;
    if (field instanceof BytesWritable) {
      BytesWritable b = (BytesWritable) field;
      try {
        row = Text.decode(b.getBytes(), 0, b.getLength());
      } catch (CharacterCodingException e) {
        throw new SerDeException(e);
      }
    } else if (field instanceof Text) {
      row = field.toString();
    }
    try {
      // Construct and return the S3LogStruct from the row data
      cachedStruct.parse(row);
      return cachedStruct;
    } catch (ClassCastException e) {
      throw new SerDeException(this.getClass().getName() + " expects Text or BytesWritable", e);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Getters
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Retrieve statistics for this SerDe. Returns null
   * because we don't support statistics (yet).
   *
   * @return The SerDe's statistics (null in this case)
   */
  @Override
  public SerDeStats getSerDeStats() { return null; }

  /**
   * Get the object inspector that can be used to navigate through the internal
   * structure of the Object returned from deserialize(...).
   *
   * @return The ObjectInspector for this Deserializer 
   * @throws SerDeException For any exception during initialization
   */
  @Override
  public ObjectInspector getObjectInspector() throws SerDeException { return cachedObjectInspector; }
}
