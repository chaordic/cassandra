/*
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
package org.apache.cassandra.utils;

import java.util.Enumeration;
import java.util.Hashtable;

import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import sun.net.util.IPAddressUtil;


public class DnsUtil {

    /**
     * Performs a reverse DNS lookup of an IP address
     * by querying the system configured DNS server
     * via JNDI.
     *
     * If the provided argument is not an IPV4 address,
     * the method will return null.
     *
     * If the provided argument is a valid IPV4 address,
     * but does not have an associated hostname, the
     * textual representation of the IP address will
     * be returned (as provided in the input).
     *
     * @param ipAddress the ip to perform reverse lookup
     * @return the hostname associated with the ipAddress
     */
    public static String reverseLookup(String ipAddress)
    {
        if (!IPAddressUtil.isIPv4LiteralAddress(ipAddress)) {
            return null;
        }

        String[] ipBytes = ipAddress.split("\\.");
        String reverseDnsDomain = ipBytes[3] + "." + ipBytes[2] + "." + ipBytes[1] + "." + ipBytes[0] + ".in-addr.arpa";

        Hashtable<String, String> env = new Hashtable<String, String>();
        env.put("java.naming.factory.initial","com.sun.jndi.dns.DnsContextFactory");
        try
        {
            DirContext ctx = new InitialDirContext(env);
            Attributes attrs = ctx.getAttributes(reverseDnsDomain,new String[] {"PTR"});
            for (NamingEnumeration<? extends Attribute> ae = attrs.getAll(); ae.hasMoreElements();)
            {
                Attribute attr = (Attribute)ae.next();
                String attrId = attr.getID();
                for (Enumeration<?> vals = attr.getAll(); vals.hasMoreElements();)
                {
                    String value = vals.nextElement().toString();
                    if ("PTR".equals(attrId))
                    {
                        final int len = value.length();
                        if (value.charAt(len - 1) == '.')
                        {
                            return value.substring(0, len - 1); // Strip out trailing period
                        }
                    }
                }
            }
            ctx.close();
        } catch(Exception e)
        {
            return ipAddress; //on exception return original IP address
        }

        return ipAddress; //if DNS query returns no result, return IP address
    }
}
