package org.apache.cassandra.utils;

import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;

public class Ec2Util {

    private static final String PRIVATE_IP_QUERY_URL = "http://169.254.169.254/latest/meta-data/local-ipv4";

    /**
     * Checks if this JVM is running on an EC2 node.
     *
     * This method assumes that this JVM is running on an
     * EC2 node if a call to the EC2 instance metadata
     * query URL executes without errors.
     *
     * @return whether this JVM is running on an EC2 node.
     */
    public static boolean isRunningOnEc2() {
        try {
            awsApiCall(PRIVATE_IP_QUERY_URL);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static String awsApiCall(String url) throws IOException, ConfigurationException
    {
        // Populate the region and zone by introspection, fail if 404 on metadata
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        DataInputStream d = null;
        try
        {
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() != 200)
                throw new ConfigurationException("Ec2Util was unable to execute the API call. Not an ec2 node?");

            // Read the information. I wish I could say (String) conn.getContent() here...
            int cl = conn.getContentLength();
            byte[] b = new byte[cl];
            d = new DataInputStream((FilterInputStream) conn.getContent());
            d.readFully(b);
            return new String(b, StandardCharsets.UTF_8);
        }
        finally
        {
            FileUtils.close(d);
            conn.disconnect();
        }
    }

}
