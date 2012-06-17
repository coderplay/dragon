/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.realtime.security;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;


/**
 * This class provides user facing APIs for transferring secrets from
 * the job client to the tasks.
 * The secrets can be stored just before submission of jobs and read during
 * the task execution.  
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TokenCache {
  
  private static final Log LOG = LogFactory.getLog(TokenCache.class);

  
  /**
   * auxiliary method to get user's secret keys..
   * @param alias
   * @return secret key from the storage
   */
  public static byte[] getSecretKey(Credentials credentials, Text alias) {
    if(credentials == null)
      return null;
    return credentials.getSecretKey(alias);
  }
  
  /**
   * Convenience method to obtain delegation tokens from namenodes 
   * corresponding to the paths passed.
   * @param credentials
   * @param ps array of paths
   * @param conf configuration
   * @throws IOException
   */
  public static void obtainTokensForNamenodes(Credentials credentials,
      Path[] ps, Configuration conf) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    obtainTokensForNamenodesInternal(credentials, ps, conf);
  }

  /**
   * Remove jobtoken referrals which don't make sense in the context
   * of the task execution.
   *
   * @param conf
   */
  public static void cleanUpTokenReferral(Configuration conf) {
    conf.unset(DragonJobConfig.DRAGON_JOB_CREDENTIALS_BINARY);
  }

  static void obtainTokensForNamenodesInternal(Credentials credentials,
      Path[] ps, Configuration conf) throws IOException {
    for(Path p: ps) {
      FileSystem fs = FileSystem.get(p.toUri(), conf);
      obtainTokensForNamenodesInternal(fs, credentials, conf);
    }
  }


  /**
   * get delegation token for a specific FS
   * @param fs
   * @param credentials
   * @param p
   * @param conf
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  static void obtainTokensForNamenodesInternal(FileSystem fs, 
      Credentials credentials, Configuration conf) throws IOException {
    // FIXME: replace with kerberos principal from resource manager 
    String delegTokenRenewer = conf.get(DragonJobConfig.JOB_SUBMITHOST);
    if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
      throw new IOException(
          "Can't get Master Kerberos principal for use as renewer");
    }
    mergeBinaryTokens(credentials, conf);

    String fsName = fs.getCanonicalServiceName();
    if (TokenCache.getDelegationToken(credentials, fsName) == null) {
      List<Token<?>> tokens =
          fs.getDelegationTokens(delegTokenRenewer, credentials);
      if (tokens != null) {
        for (Token<?> token : tokens) {
          credentials.addToken(token.getService(), token);
          LOG.info("Got dt for " + fs.getUri() + ";uri="+ fsName + 
              ";t.service="+token.getService());
        }
      }
      //Call getDelegationToken as well for now - for FS implementations
      // which may not have implmented getDelegationTokens (hftp)
      if (tokens == null || tokens.size() == 0) {
        Token<?> token = fs.getDelegationToken(delegTokenRenewer);
        if (token != null) {
          credentials.addToken(token.getService(), token);
          LOG.info("Got dt for " + fs.getUri() + ";uri=" + fsName
              + ";t.service=" + token.getService());
        }
      }
    }
  }
 
  private static void mergeBinaryTokens(Credentials creds, Configuration conf) {
    String binaryTokenFilename =
        conf.get(DragonJobConfig.DRAGON_JOB_CREDENTIALS_BINARY);
    if (binaryTokenFilename != null) {
      Credentials binary;
      try {
        binary = Credentials.readTokenStorageFile(
            new Path("file:///" +  binaryTokenFilename), conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      // supplement existing tokens with the tokens in the binary file
      creds.addAll(binary);
    }
  }
  
  /**
   * file name used on HDFS for generated job token
   */
  @InterfaceAudience.Private
  public static final String JOB_TOKEN_HDFS_FILE = "jobToken";

  /**
   * conf setting for job tokens cache file name
   */
  @InterfaceAudience.Private
  public static final String JOB_TOKENS_FILENAME = "mapreduce.job.jobTokenFile";
  private static final Text JOB_TOKEN = new Text("ShuffleAndJobToken");
  
  /**
   * 
   * @param namenode
   * @return delegation token
   */
  @SuppressWarnings("unchecked")
  @InterfaceAudience.Private
  public static Token<DelegationTokenIdentifier> getDelegationToken(
      Credentials credentials, String namenode) {
    //No fs specific tokens issues by this fs. It may however issue tokens
    // for other filesystems - which would be keyed by that filesystems name.
    if (namenode == null)  
      return null;
    return (Token<DelegationTokenIdentifier>) credentials.getToken(new Text(
        namenode));
  }

  /**
   * store job token
   * @param t
   */
  @InterfaceAudience.Private
  public static void setJobToken(Token<? extends TokenIdentifier> t, 
      Credentials credentials) {
    credentials.addToken(JOB_TOKEN, t);
  }

}
