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

package org.apache.zookeeper.client;

import org.apache.zookeeper.common.PathUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.apache.zookeeper.common.StringUtils.split;

/**
 * A parser for ZooKeeper Client connect strings.
 * ZooKeeper客户端连接字符串的分析器
 * This class is not meant to be seen or used outside of ZooKeeper itself.
 * 这个类不应该在ZooKeeper本身之外被看到或使用
 * The chrootPath member should be replaced by a Path object in issue
 * ZOOKEEPER-849.
 * 
 * @see org.apache.zookeeper.ZooKeeper
 */
//对传入的connectString做两个主要处理，解析chrootpath，保存服务器地址列表
public final class ConnectStringParser {
    private static final int DEFAULT_PORT = 2181;

    //客户端隔离命名空间，传给ClientCnxn.chrootPath
    private final String chrootPath;

    private final ArrayList<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>();

    /**
     * 
     * @throws IllegalArgumentException
     *             for an invalid chroot path.
     */
    public ConnectStringParser(String connectString) {
        // parse out chroot, if any
        int off = connectString.indexOf('/');
        if (off >= 0) {
            String chrootPath = connectString.substring(off);
            // ignore "/" chroot spec, same as null
            if (chrootPath.length() == 1) {
                this.chrootPath = null;
            } else {
                PathUtils.validatePath(chrootPath);
                this.chrootPath = chrootPath;
            }
            connectString = connectString.substring(0, off);
        } else {
            this.chrootPath = null;
        }
        //根据','分割
        List<String> hostsList = split(connectString,",");
        for (String host : hostsList) {
            int port = DEFAULT_PORT;
            int pidx = host.lastIndexOf(':');
            if (pidx >= 0) {
                // otherwise : is at the end of the string, ignore
                if (pidx < host.length() - 1) {
                    port = Integer.parseInt(host.substring(pidx + 1));
                }
                host = host.substring(0, pidx);
            }
            serverAddresses.add(InetSocketAddress.createUnresolved(host, port));
        }
    }

    public String getChrootPath() {
        return chrootPath;
    }

    public ArrayList<InetSocketAddress> getServerAddresses() {
        return serverAddresses;
    }
}
