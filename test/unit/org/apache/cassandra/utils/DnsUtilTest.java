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

import static org.junit.Assert.*;

import java.net.InetAddress;

import org.junit.Test;

public class DnsUtilTest {

    @Test
    public void reverseLookupLocalhost()
    {
        InetAddress loopbackAddress = InetAddress.getLoopbackAddress();
        assertEquals("localhost", DnsUtil.reverseLookup(loopbackAddress.getHostAddress()));
    }

    @Test
    public void reverseLookupInvalidAddress()
    {
        assertNull(DnsUtil.reverseLookup("foo"));
    }

    @Test
    public void reverseLookupUnmappedIp()
    {
        assertEquals("255.255.255.255", DnsUtil.reverseLookup("255.255.255.255"));
    }

    @Test
    public void reverseLookupGoogleDns()
    {
        String googleDnsHostname = "google-public-dns-a.google.com";
        String googleDnsIp = "8.8.8.8";
        assertEquals(googleDnsHostname, DnsUtil.reverseLookup(googleDnsIp));
    }
}
