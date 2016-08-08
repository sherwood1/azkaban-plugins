/*
 * Copyright 2014 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.jobtype;

import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;

// delete later
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PrepareStatement;
import java.sql.ResultSet;

/**
 * HadoopConfigurationInjector is responsible for inserting links back to the
 * Azkaban UI in configurations and for automatically injecting designated job
 * properties into the Hadoop configuration.
 * <p>
 * It is assumed that the necessary links have already been loaded into the
 * properties. After writing the necessary links as a xml file as required by
 * Hadoop's configuration, clients may add the links as a default resource
 * using injectResources() so that they are included in any Configuration
 * constructed.
 */
public class HadoopConfigurationInjector {
  private static Logger _logger = Logger.getLogger(HadoopConfigurationInjector.class);

  // File to which the Hadoop configuration to inject will be written.
  private static final String INJECT_FILE = "hadoop-inject.xml";

  // Prefix for properties to be automatically injected into the Hadoop conf.
  public static final String INJECT_PREFIX = "hadoop-inject.";

  // Key for job name
  public static final String AZKABAN_JOB_NAME_KEY = "azkaban.job.id";

  // Key for workflow name
  public static final String AZKABAN_WORKFLOW_NAME_KEY = "azkaban.flow.flowid";

  // Key for project name
  public static final String AZKABAN_PROJECT_NAME_KEY = "azkaban.flow.projectname";

  // Key for user's consent to run on Rain hosts
  public static final Boolean RAIN_USER_PERMISSION = "elastic.consent";

  // Key for If elastic grid feature is turned on
  public static final Boolean RAIN_ENABLE= "elastic.switch";

  // Key for CandidateJob DB hostname
  public static final String RAIN_DB_HOSTNAME = "elastic.dbhostname";

  // Key for CandidateJob Database name
  public static final String RAIN_DB_NAME = "elastic.database";

  // Key for CandidateJob Table name
  public static final String RAIN_TABLE_NAME = "elastic.table";

  // Key for username to Candidate Job Databse
  public static final String RAIN_DB_USERNAME = "elastic.usernameDB";

  // Key fro password to Candidate Job Database
  public static final String RAIN_DB_PASSWORD = "elastic.passwordDB";


  /*
   * To be called by the forked process to load the generated links and Hadoop
   * configuration properties to automatically inject.
   *
   * @param props The Azkaban properties
   */
  public static void injectResources(Props props) {
    // Add mapred, yarn and hdfs site configs (in addition to core-site, which
    // is automatically added) as default resources before we add the injected
    // configuration. This will cause the injected properties to override the
    // default site properties (instead of vice-versa). This is safe to do,
    // even when these site files don't exist for your Hadoop installation.
    if (props.getBoolean("azkaban.inject.hadoop-site.configs", true)) {
      Configuration.addDefaultResource("mapred-default.xml");
      Configuration.addDefaultResource("mapred-site.xml");
      Configuration.addDefaultResource("yarn-default.xml");
      Configuration.addDefaultResource("yarn-site.xml");
      Configuration.addDefaultResource("hdfs-default.xml");
      Configuration.addDefaultResource("hdfs-site.xml");
    }
    Configuration.addDefaultResource(INJECT_FILE);
  }

  /**
   * Writes out the XML configuration file that will be injected by the client
   * as a configuration resource.
   * <p>
   * This file will include a series of links injected by Azkaban as well as
   * any job properties that begin with the designated injection prefix.
   *
   * @param props The Azkaban properties
   * @param sysProps The Azkaban system properties
   * @param workingDir The Azkaban job working directory
   */
  public static void prepareResourcesToInject(Props props, Props sysProps, String workingDir) {
    PrepareStatement stmt = null;
    ResultSet rs = null;

    try {
      Configuration conf = new Configuration(false);

      // First, inject a series of Azkaban links. These are equivalent to
      // CommonJobProperties.[EXECUTION,WORKFLOW,JOB,JOBEXEC,ATTEMPT]_LINK
      addHadoopProperties(props);

      // Next, automatically inject any properties that begin with the
      // designated injection prefix.
      Map<String, String> confProperties = props.getMapByPrefix(INJECT_PREFIX);

      for (Map.Entry<String, String> entry : confProperties.entrySet()) {
        String confKey = entry.getKey().replace(INJECT_PREFIX, "");
        String confVal = entry.getValue();
        if (confVal != null) {
          conf.set(confKey, confVal);
        }
      }

      // delete later
      PrintWriter writer = new PrintWriter("sherwood.txt", "UTF-8");
      writer.println("jobProps: ");
      for (String s: props.localKeySet()) {
        String value = props.get(s);
        writer.println("key: " + s);
        writer.println("value: " + value);
      }
      writer.println("sysProps: ");
      for (String s: sysProps.localKeySet()) {
        String value = sysProps.get(s);
        writer.println("key: " + s);
        writer.println("value: " + value);
      }

      writer.close();

      // Get submitted job information
      String projectName = props.get(AZKABAN_PROJECT_NAME_KEY);
      String flowName = props.get(AZKABAN_WORKFLOW_NAME_KEY);
      String jobName = props.get(AZKABAN_JOB_NAME_KEY);
      String userPermission = props.getBoolean(RAIN_USER_PERMISSION);

      // get elastic grid related system configuration
      String rainEnable = sysProps.getBoolean(RAIN_ENABLE);
      String userName = sysProps.get(RAIN_DB_USERNAME);
      String password = sysProps.get(RAIN_DB_PASSWORD);
      String dbHostname = sysProps.get(RAIN_DB_HOSTNAME);
      String databaseName = sysProps.get(RAIN_DB_NAME);
      String tableName = sysProps.get(RAIN_TABLE_NAME);

      // Check if the job is suitable for running on online machines
      if (elasticGrid != null && jobName != null && flowName != null && ProjectName != null
          && !elasticGrid.equals("") && !jobName.equals("") && !flowName.equals("")
          && !projectName.equals("") && rainEnable
          && !userConsent.equals("") && userPermission) {

        // initialize the connection to mysql database
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        Connection conn = DriverManager.getConnection("jdbc:mysql://" + dbHostname + "/" + databaseName,
            userName, password);

        // user prepare statement for security and performance
        stmt = conn.prepareStatement("SELECT * FROM ? WHERE jobname=? and flowname=? and "
            + "projectname =?");
        stmt.setString(1, tableName);
        stmt.setString(2, jobName);
        stmt.setString(3, flowName);
        stmt.setString(4, projectName);

        rs = stmt.executeQuery();

        if (rs.next()) {
          // This Azkaban job is one of candidate jobs, submit to Rain host
          // For testing purposes, will replace these magic words later
          conf.set("mapred.job.queue.name", "highlight");
        }
      }

      // Now write out the configuration file to inject.
      File file = getConfFile(props, workingDir, INJECT_FILE);
      OutputStream xmlOut = new FileOutputStream(file);
      conf.writeXml(xmlOut);
      xmlOut.close();
    } catch (Throwable e) {
      _logger.error("Encountered error while preparing the Hadoop configuration resource file", e);
    } finally {
      // release resources in a finally{} block
      // in reverse-order of their creation
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException sqlEx) {
          _logger.error(sqlEx.getMessage());
        }

        rs = null;
      }

      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException sqlEx) {
          _logger.error(sqlEx.getMessage());
        }

        stmt = null;
      }

      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException sqlEx) {
          _logger.error(sqlEx.getMessage());
        }

        conn = null;
      }
    }
  }

  private static void addHadoopProperty(Props props, String propertyName) {
      props.put(INJECT_PREFIX + propertyName, props.get(propertyName));
  }

  private static void addHadoopProperties(Props props) {
    String[] propsToInject = new String[]{
        CommonJobProperties.EXEC_ID,
        CommonJobProperties.FLOW_ID,
        CommonJobProperties.JOB_ID,
        CommonJobProperties.PROJECT_NAME,
        CommonJobProperties.PROJECT_VERSION,
        CommonJobProperties.EXECUTION_LINK,
        CommonJobProperties.JOB_LINK,
        CommonJobProperties.WORKFLOW_LINK,
        CommonJobProperties.JOBEXEC_LINK,
        CommonJobProperties.ATTEMPT_LINK,
        CommonJobProperties.OUT_NODES,
        CommonJobProperties.IN_NODES,
        CommonJobProperties.PROJECT_LAST_CHANGED_DATE,
        CommonJobProperties.PROJECT_LAST_CHANGED_BY,
        CommonJobProperties.SUBMIT_USER
    };

    for(String propertyName : propsToInject) {
      addHadoopProperty(props, propertyName);
    }
  }

  /**
   * Resolve the location of the file containing the configuration file.
   *
   * @param props The Azkaban properties
   * @param workingDir The Azkaban job working directory
   * @param fileName The desired configuration file name
   */
  public static File getConfFile(Props props, String workingDir, String fileName) {
    File jobDir = new File(workingDir, getDirName(props));
    if (!jobDir.exists()) {
      jobDir.mkdir();
    }
    return new File(jobDir, fileName);
  }

  /**
   * For classpath reasons, we'll put each link file in a separate directory.
   * This must be called only after the job id has been inserted by the job.
   *
   * @param props The Azkaban properties
   */
  public static String getDirName(Props props) {
    String dirSuffix = props.get(CommonJobProperties.NESTED_FLOW_PATH);

    if ((dirSuffix == null) || (dirSuffix.length() == 0)) {
      dirSuffix = props.get(CommonJobProperties.JOB_ID);
      if ((dirSuffix == null) || (dirSuffix.length() == 0)) {
        throw new RuntimeException("azkaban.flow.nested.path and azkaban.job.id were not set");
      }
    }

    return "_resources_" + dirSuffix.replace(':', '_');
  }

  /**
   * Gets the path to the directory in which the generated links and Hadoop
   * conf properties files are written.
   *
   * @param props The Azkaban properties
   * @param workingDir The Azkaban job working directory
   */
  public static String getPath(Props props, String workingDir) {
    return new File(workingDir, getDirName(props)).toString();
  }

  /**
   * Loads an Azkaban property into the Hadoop configuration.
   *
   * @param props The Azkaban properties
   * @param conf The Hadoop configuration
   * @param name The property name to load from the Azkaban properties into the Hadoop configuration
   */
  public static void loadProp(Props props, Configuration conf, String name) {
    String prop = props.get(name);
    if (prop != null) {
      conf.set(name, prop);
    }
  }
}
