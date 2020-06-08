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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Most simple HostProvider, resolves on every next() call.
 * Please be aware that although this class doesn't do any DNS caching, there're multiple levels of caching already
 * present across the stack like in JVM, OS level, hardware, etc. The best we could do here is to get the most recent
 * address from the underlying system which is considered up-to-date.
 * 调用next()的实现类
 * 这个类没有做任何DNS缓存，但是已经有多个级别的缓存，jvm，操作系统，硬件层面
 * 其实核心方法就是取最新的地址
 */
@InterfaceAudience.Public
//提供服务器列表
public final class StaticHostProvider implements HostProvider {
    public interface Resolver {
        InetAddress[] getAllByName(String name) throws UnknownHostException;
    }

    private static final Logger LOG = LoggerFactory
            .getLogger(StaticHostProvider.class);

    private List<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>(
            5);//没有被解析的服务器地址

    private Random sourceOfRandomness;
    private int lastIndex = -1;//当前服务器正在使用的服务器地址index

    private int currentIndex = -1;//当前循环队列遍历到的元素index

    /**
     * The following fields are used to migrate clients during reconfiguration
     * 更新了地址列表，然后此时要去获取一个地址去链接,此时涉及到当前服务端的负载均衡(是否将当前客户端负载均衡到新的服务端)
     * true:需要负载均衡到新的服务端
     * false:不需要负载均衡，继续保持当前服务端链接
     */
    private boolean reconfigMode = false;

    private final List<InetSocketAddress> oldServers = new ArrayList<InetSocketAddress>(
            5);

    private final List<InetSocketAddress> newServers = new ArrayList<InetSocketAddress>(
            5);

    private int currentIndexOld = -1;
    private int currentIndexNew = -1;

    //负载均衡连接到新服务端的概率大小
    //pNew=1-->一定连接到新服务器
    private float pOld, pNew;

    private Resolver resolver;

    /**
     * Constructs a SimpleHostSet.
     * 
     * @param serverAddresses 未被解析的服务端地址
     *            possibly unresolved ZooKeeper server addresses
     * @throws IllegalArgumentException
     *             if serverAddresses is empty or resolves to an empty list
     */
    public StaticHostProvider(Collection<InetSocketAddress> serverAddresses) {
        init(serverAddresses,
                System.currentTimeMillis() ^ this.hashCode(),
                new Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) throws UnknownHostException {
                return InetAddress.getAllByName(name);
            }
        });
    }

    /**
     * Constructs a SimpleHostSet.
     *
     * Introduced for testing purposes. getAllByName() is a static method of InetAddress, therefore cannot be easily mocked.
     * By abstraction of Resolver interface we can easily inject a mocked implementation in tests.
     * 测试方法，getAllByName()是一个静态方法，不容易被模拟，通过对解析器接口的抽象，我们可以很容易地在测试中注入模拟实现
     *
     * @param serverAddresses
     *              possibly unresolved ZooKeeper server addresses
     * @param resolver
     *              custom resolver implementation
     */
    public StaticHostProvider(Collection<InetSocketAddress> serverAddresses, Resolver resolver) {
        init(serverAddresses, System.currentTimeMillis() ^ this.hashCode(), resolver);
    }

    /**
     * Constructs a SimpleHostSet. This constructor is used from StaticHostProviderTest to produce deterministic test results
     * by initializing sourceOfRandomness with the same seed
     * 
     * @param serverAddresses
     *            possibly unresolved ZooKeeper server addresses
     * @param randomnessSeed a seed used to initialize sourceOfRandomnes
     * @throws IllegalArgumentException
     *             if serverAddresses is empty or resolves to an empty list
     */
    public StaticHostProvider(Collection<InetSocketAddress> serverAddresses,
        long randomnessSeed) {
        init(serverAddresses, randomnessSeed, new Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) throws UnknownHostException {
                return InetAddress.getAllByName(name);
            }
        });
    }

    /**
     * 解析并放入{@link serverAddresses}，使用Collections.shuffle对服务器地址随机打散
     * @param serverAddresses
     * @param randomnessSeed
     * @param resolver
     */
    private void init(Collection<InetSocketAddress> serverAddresses, long randomnessSeed, Resolver resolver) {
        this.sourceOfRandomness = new Random(randomnessSeed);
        this.resolver = resolver;
        if (serverAddresses.isEmpty()) {
            throw new IllegalArgumentException(
                    "A HostProvider may not be empty!");
        }
        this.serverAddresses = shuffle(serverAddresses);
        currentIndex = -1;
        lastIndex = -1;
    }

    private InetSocketAddress resolve(InetSocketAddress address) {
        try {
            String curHostString = address.getHostString();
            List<InetAddress> resolvedAddresses = new ArrayList<>(Arrays.asList(this.resolver.getAllByName(curHostString)));
            if (resolvedAddresses.isEmpty()) {
                return address;
            }
            Collections.shuffle(resolvedAddresses);
            return new InetSocketAddress(resolvedAddresses.get(0), address.getPort());
        } catch (UnknownHostException e) {
            LOG.error("Unable to resolve address: {}", address.toString(), e);
            return address;
        }
    }

    private List<InetSocketAddress> shuffle(Collection<InetSocketAddress> serverAddresses) {
        List<InetSocketAddress> tmpList = new ArrayList<>(serverAddresses.size());
        tmpList.addAll(serverAddresses);
        Collections.shuffle(tmpList, sourceOfRandomness);
        return tmpList;
    }

    /**
     * Update the list of servers. This returns true if changing connections is necessary for load-balancing, false
	 * otherwise. Changing connections is necessary if one of the following holds: 
     * a) the host to which this client is currently connected is not in serverAddresses.
     *    Otherwise (if currentHost is in the new list serverAddresses):   
     * b) the number of servers in the cluster is increasing - in this case the load on currentHost should decrease,
     *    which means that SOME of the clients connected to it will migrate to the new servers. The decision whether
     *    this client migrates or not (i.e., whether true or false is returned) is probabilistic so that the expected 
     *    number of clients connected to each server is the same.
     * 更新服务器列表。如果负载平衡需要更改连接，则返回true
     * 如果下列情况之一成立，则需要更改连接：
     * a) 此客户端当前连接的主机不在服务器地址中(或者当前主机在新的列表服务器地址中)
     * b) 集群中的服务器数量正在增加-在这种情况下，当前主机上的负载应该减少，
     * 这意味着连接到它的一些客户机将迁移到新服务器。决定是否
     * 此客户端迁移与否（即是否返回true或false）是概率的，因此期望连接到每台服务器的客户端数相同
     *    
     * If true is returned, the function sets pOld and pNew that correspond to the probability to migrate to ones of the
     * new servers in serverAddresses or one of the old servers (migrating to one of the old servers is done only
     * if our client's currentHost is not in serverAddresses). See nextHostInReconfigMode for the selection logic.
     * 如果返回true，则函数设置pOld和pNew，它们对应于迁移到
     * 服务器地址中的新服务器或旧服务器之一（仅迁移到旧服务器之一如果客户端的当前主机不在服务器地址中）
     * See <a href="https://issues.apache.org/jira/browse/ZOOKEEPER-1355">ZOOKEEPER-1355</a>
     * for the protocol and its evaluation, and StaticHostProviderTest for the tests that illustrate how load balancing
     * works with this policy.
     * @param serverAddresses new host list 新的服务端地址列表
     * @param currentHost the host to which this client is currently connected 当前客户端链接的地址
     * @return true if changing connections is necessary for load-balancing, false otherwise  
     */
    @Override
    //根据新增server决定平衡状态，
    public synchronized boolean updateServerList(
            Collection<InetSocketAddress> serverAddresses,
            InetSocketAddress currentHost) {
        List<InetSocketAddress> shuffledList = shuffle(serverAddresses);//打乱顺序的新的服务端地址列表
        if (shuffledList.isEmpty()) {
            throw new IllegalArgumentException(
                    "A HostProvider may not be empty!");
        }
        // Check if client's current server is in the new list of servers
        boolean myServerInNewConfig = false;//当前连接的服务端地址是否在新地址列表flag

        InetSocketAddress myServer = currentHost;//当前服务端地址

        // choose "current" server according to the client rebalancing algorithm
        //查看配置是否需要一定重新链接server，挑选下一个可以连接的server连接
        if (reconfigMode) {
            myServer = next(0);
        }

        // if the client is not currently connected to any server
        //没有链接任何server
        if (myServer == null) {
            // reconfigMode = false (next shouldn't return null).
            if (lastIndex >= 0) {
                // take the last server to which we were connected
                //最后一个server
                myServer = this.serverAddresses.get(lastIndex);
            } else {
                //第一个server
                myServer = this.serverAddresses.get(0);
            }
        }

        //判断条件1：此客户端当前连接的主机不在旧服务器地址中(或者当前主机在新的列表服务器地址中)
        for (InetSocketAddress addr : shuffledList) {
            if (addr.getPort() == myServer.getPort()
                    && ((addr.getAddress() != null
                            && myServer.getAddress() != null && addr
                            .getAddress().equals(myServer.getAddress())) || addr
                            .getHostString().equals(myServer.getHostString()))) {
                myServerInNewConfig = true;
                break;
            }
        }

        reconfigMode = true;

        newServers.clear();
        oldServers.clear();
        // Divide the new servers into oldServers that were in the previous list
        // and newServers that were not in the previous list
        //将新服务器分成前一个列表中的旧服务器
        //以及不在上一个列表中的新服务器
        //重新计算newserver和oldserver
        for (InetSocketAddress address : shuffledList) {
            if (this.serverAddresses.contains(address)) {
                oldServers.add(address);
            } else {
                newServers.add(address);
            }
        }

        int numOld = oldServers.size();
        int numNew = newServers.size();

        //判断条件2：集群中的服务器数量正在增加-在这种情况下，当前主机上的负载应该减少
        // number of servers increased
        //总结
        if (numOld + numNew > this.serverAddresses.size()) {
            if (myServerInNewConfig) {
                // my server is in new config, but load should be decreased.
                // Need to decide if this client
                // is moving to one of the new servers
                if (sourceOfRandomness.nextFloat() <= (1 - ((float) this.serverAddresses
                        .size()) / (numOld + numNew))) {
                    pNew = 1;
                    pOld = 0;
                } else {
                    // do nothing special - stay with the current server
                    reconfigMode = false;
                }
            } else {
                // my server is not in new config, and load on old servers must
                // be decreased, so connect to
                // one of the new servers
                pNew = 1;
                pOld = 0;
            }
        } else { // number of servers stayed the same or decreased
            if (myServerInNewConfig) {
                // my server is in new config, and load should be increased, so
                // stay with this server and do nothing special
                reconfigMode = false;
            } else {
                pOld = ((float) (numOld * (this.serverAddresses.size() - (numOld + numNew))))
                        / ((numOld + numNew) * (this.serverAddresses.size() - numOld));
                pNew = 1 - pOld;
            }
        }

        if (!reconfigMode) {
            currentIndex = shuffledList.indexOf(getServerAtCurrentIndex());
        } else {
            currentIndex = -1;
        }
        this.serverAddresses = shuffledList;
        currentIndexOld = -1;
        currentIndexNew = -1;
        lastIndex = currentIndex;
        return reconfigMode;
    }

    public synchronized InetSocketAddress getServerAtIndex(int i) {
    	if (i < 0 || i >= serverAddresses.size()) return null;
    	return serverAddresses.get(i);
    }
    
    public synchronized InetSocketAddress getServerAtCurrentIndex() {
    	return getServerAtIndex(currentIndex);
    }

    public synchronized int size() {
        return serverAddresses.size();
    }

    /**
     * Get the next server to connect to, when in "reconfigMode", which means that 
     * you've just updated the server list, and now trying to find some server to connect to. 
     * Once onConnected() is called, reconfigMode is set to false. Similarly, if we tried to connect
     * to all servers in new config and failed, reconfigMode is set to false.
     * 刚刚更新了服务器列表，然后尝试查找要连接的服务器，调用onConnected()后，reconfigMode设置为false。同样，如果我们试图联系
     * 对于新配置中所有失败的服务器，reconfigMode设置为false。
     * While in reconfigMode, we should connect to a server in newServers with probability pNew and to servers in
     * oldServers with probability pOld (which is just 1-pNew). If we tried out all servers in either oldServers
     * or newServers we continue to try servers from the other set, regardless of pNew or pOld. If we tried all servers
     * we give up and go back to the normal round robin mode
     * 在reconfigMode模式中，我们应该连接到概率为pNew的新服务器中的服务器和具有概率pOld（仅为1-pNew）的旧服务器
     * 如果我们在任何一个旧服务器上试用所有服务器或者继续尝试来自另一组的新服务器，不管新的旧的。
     * 如果我们试过所有的服务器，我们放弃回到正常的循环模式
     * When called, this should be protected by synchronized(this)
     * 同步方法
     */
    private InetSocketAddress nextHostInReconfigMode() {
        boolean takeNew = (sourceOfRandomness.nextFloat() <= pNew);

        // take one of the new servers if it is possible (there are still such
        // servers we didn't try),
        // and either the probability tells us to connect to one of the new
        // servers or if we already
        // tried all the old servers
        //如果没有尝试玩所有新的服务器，尝试链接一个新的server
        //或者在尝试了所有的旧服务器后根据概率链接新服务器
        if (((currentIndexNew + 1) < newServers.size())
                && (takeNew || (currentIndexOld + 1) >= oldServers.size())) {
            ++currentIndexNew;
            return newServers.get(currentIndexNew);
        }

        // start taking old servers
        if ((currentIndexOld + 1) < oldServers.size()) {
            ++currentIndexOld;
            return oldServers.get(currentIndexOld);
        }

        return null;
    }

    //新的服务端地址列表，需要进行负载均衡
    public InetSocketAddress next(long spinDelay) {
        boolean needToSleep = false;
        InetSocketAddress addr;

        synchronized(this) {
            if (reconfigMode) {
                addr = nextHostInReconfigMode();
                if (addr != null) {
                	currentIndex = serverAddresses.indexOf(addr);
                	return resolve(addr);
                }
                //tried all servers and couldn't connect
                reconfigMode = false;
                needToSleep = (spinDelay > 0);
            }        
            ++currentIndex;
            if (currentIndex == serverAddresses.size()) {//循环
                currentIndex = 0;
            }            
            addr = serverAddresses.get(currentIndex);
            needToSleep = needToSleep || (currentIndex == lastIndex && spinDelay > 0);
            if (lastIndex == -1) { //第一次连接不需要sleep
                // We don't want to sleep on the first ever connect attempt.
                lastIndex = 0;
            }
        }
        if (needToSleep) {
            try {
                Thread.sleep(spinDelay);
            } catch (InterruptedException e) {
                LOG.warn("Unexpected exception", e);
            }
        }

        return resolve(addr);
    }

    public synchronized void onConnected() {
        lastIndex = currentIndex;
        reconfigMode = false;
    }

}
