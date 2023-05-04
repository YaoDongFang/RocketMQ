/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.netty;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.resolver.NoopAddressResolverGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.proxy.SocksProxyConfig;

public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    private final NettyClientConfig nettyClientConfig;
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private final Lock lockChannelTables = new ReentrantLock();
    private final Map<String /* cidr */, SocksProxyConfig /* proxy */> proxyMap = new HashMap<>();
    private final ConcurrentHashMap<String /* cidr */, Bootstrap> bootstrapMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String /* addr */, ChannelWrapper> channelTables = new ConcurrentHashMap<>();

    private final HashedWheelTimer timer = new HashedWheelTimer(r -> new Thread(r, "ClientHouseKeepingService"));

    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<>();
    private final ConcurrentMap<String, Boolean> availableNamesrvAddrMap = new ConcurrentHashMap<>();
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<>();
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
    private final Lock namesrvChannelLock = new ReentrantLock();

    private final ExecutorService publicExecutor;
    private final ExecutorService scanExecutor;

    /**
     * Invoke the callback methods in this executor when process response.
     */
    private ExecutorService callbackExecutor;
    private final ChannelEventListener channelEventListener;
    private EventExecutorGroup defaultEventExecutorGroup;

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener) {
        this(nettyClientConfig, channelEventListener, null, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener,
        final EventLoopGroup eventLoopGroup,
        final EventExecutorGroup eventExecutorGroup) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        this.loadSocksProxyJson();

        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactoryImpl("NettyClientPublicExecutor_"));

        this.scanExecutor = new ThreadPoolExecutor(4, 10, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(32), new ThreadFactoryImpl("NettyClientScan_thread_"));

        if (eventLoopGroup != null) {
            this.eventLoopGroupWorker = eventLoopGroup;
        } else {
            this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactoryImpl("NettyClientSelector_"));
        }
        this.defaultEventExecutorGroup = eventExecutorGroup;

        if (nettyClientConfig.isUseTLS()) {
            try {
                sslContext = TlsHelper.buildSslContext(true);
                LOGGER.info("SSL enabled for client");
            } catch (IOException e) {
                LOGGER.error("Failed to create SSLContext", e);
            } catch (CertificateException e) {
                LOGGER.error("Failed to create SSLContext", e);
                throw new RuntimeException("Failed to create SSLContext", e);
            }
        }
    }

    private static int initValueIndex() {
        Random r = new Random();
        return r.nextInt(999);
    }

    private void loadSocksProxyJson() {
        Map<String, SocksProxyConfig> sockProxyMap = JSON.parseObject(
            nettyClientConfig.getSocksProxyConfig(), new TypeReference<Map<String, SocksProxyConfig>>() {
            });
        if (sockProxyMap != null) {
            proxyMap.putAll(sockProxyMap);
        }
    }

    /**
     * NettyRemotingClient的方法
     * 创建netty客户端，并没有真正启动
     */
    @Override
    public void start() {
        //创建默认事件处理器组，默认4个线程，线程名以NettyClientWorkerThread_为前缀。
        //主要用于执行在真正执行业务逻辑之前需要进行的SSL验证、编解码、空闲检查、网络连接管理等操作
        //其工作时间位于IO线程组之后，process线程组之前
        if (this.defaultEventExecutorGroup == null) {
            this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                nettyClientConfig.getClientWorkerThreads(),
                new ThreadFactoryImpl("NettyClientWorkerThread_"));
        }
        /*
         * 初始化netty客户端
         */
        //eventLoopGroupWorker线程组，默认一个线程
        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
                //对应于套接字选项中的TCP_NODELAY，该参数的使用与Nagle算法有关
                .option(ChannelOption.TCP_NODELAY, true)
                //对应于套接字选项中的SO_KEEPALIVE，该参数用于设置TCP连接，当设置该选项以后，连接会测试链接的状态
                .option(ChannelOption.SO_KEEPALIVE, false)
                //用来设置连接超时时长，单位是毫秒，默认3000
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    if (nettyClientConfig.isUseTLS()) {
                        if (null != sslContext) {
                            pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
                            LOGGER.info("Prepend SSL handler");
                        } else {
                            LOGGER.warn("Connections are insecure as SSLContext is null!");
                        }
                    }
                    //为defaultEventExecutorGroup，添加handler
                    ch.pipeline().addLast(
                        nettyClientConfig.isDisableNettyWorkerGroup() ? null : defaultEventExecutorGroup,
                            //RocketMQ自定义的请求解码器
                            new NettyEncoder(),
                            //RocketMQ自定义的请求编码器
                            new NettyDecoder(),
                            //Netty自带的心跳管理器，主要是用来检测远端是否存活
                            //即测试端一定时间内未接受到被测试端消息和一定时间内向被测试端发送消息的超时时间为120秒
                        new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                            //连接管理器，他负责连接的激活、断开、超时、异常等事件
                            new NettyConnectManageHandler(),
                            //服务请求处理器，处理RemotingCommand消息，即请求和响应的业务处理，并且返回相应的处理结果。这是重点
                            //例如broker注册、producer/consumer获取Broker、Topic信息等请求都是该处理器处理
                            //serverHandler最终会将请求根据不同的消息类型code分发到不同的process线程池处理
                        new NettyClientHandler());
                }
            });
        if (nettyClientConfig.getClientSocketSndBufSize() > 0) {
            LOGGER.info("client set SO_SNDBUF to {}", nettyClientConfig.getClientSocketSndBufSize());
            handler.option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize());
        }
        if (nettyClientConfig.getClientSocketRcvBufSize() > 0) {
            LOGGER.info("client set SO_RCVBUF to {}", nettyClientConfig.getClientSocketRcvBufSize());
            handler.option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize());
        }
        if (nettyClientConfig.getWriteBufferLowWaterMark() > 0 && nettyClientConfig.getWriteBufferHighWaterMark() > 0) {
            LOGGER.info("client set netty WRITE_BUFFER_WATER_MARK to {},{}",
                nettyClientConfig.getWriteBufferLowWaterMark(), nettyClientConfig.getWriteBufferHighWaterMark());
            handler.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                nettyClientConfig.getWriteBufferLowWaterMark(), nettyClientConfig.getWriteBufferHighWaterMark()));
        }
        if (nettyClientConfig.isClientPooledByteBufAllocatorEnable()) {
            handler.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        /*
         * 启动定时任务，初始启动3秒后执行，此后每隔1秒执行一次
         * 扫描responseTable，将超时的ResponseFuture直接移除，并且执行这些超时ResponseFuture的回调
         * 这里主要是用于处理通信时的异常情况。RocketMQ会将请求结果封装为一个ResponseFuture并且存入responseTable中。
         * 那么在发送消息时候，如果遇到服务端没有response返回给客户端或者response因网络而丢失等异常情况。
         * 此时可能造成responseTable中的ResponseFuture累积，因此该任务会每隔一秒扫描一次responseTable，
         * 将超时的ResponseFuture直接移除，并且执行这些超时ResponseFuture的回调。
         */
        TimerTask timerTaskScanResponseTable = new TimerTask() {
            @Override
            public void run(Timeout timeout) {
                try {
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Throwable e) {
                    LOGGER.error("scanResponseTable exception", e);
                } finally {
                    timer.newTimeout(this, 1000, TimeUnit.MILLISECONDS);
                }
            }
        };
        this.timer.newTimeout(timerTaskScanResponseTable, 1000 * 3, TimeUnit.MILLISECONDS);

        /*
         * 启动定时任务，初始启动3秒后执行，此后每隔1秒执行一次
         * 扫描存活的name srv 更新本地NettyRemotingClient 中availableNamesrvAddrMap的信息
         * 移除不存在或不可通信的name srv 地址
         */
        int connectTimeoutMillis = this.nettyClientConfig.getConnectTimeoutMillis();
        TimerTask timerTaskScanAvailableNameSrv = new TimerTask() {
            @Override
            public void run(Timeout timeout) {
                try {
                    NettyRemotingClient.this.scanAvailableNameSrv();
                } catch (Exception e) {
                    LOGGER.error("scanAvailableNameSrv exception", e);
                } finally {
                    timer.newTimeout(this, connectTimeoutMillis, TimeUnit.MILLISECONDS);
                }
            }
        };
        this.timer.newTimeout(timerTaskScanAvailableNameSrv, 0, TimeUnit.MILLISECONDS);
    }

    private Map.Entry<String, SocksProxyConfig> getProxy(String addr) {
        if (StringUtils.isBlank(addr) || !addr.contains(":")) {
            return null;
        }
        String[] hostAndPort = this.getHostAndPort(addr);
        for (Map.Entry<String, SocksProxyConfig> entry : proxyMap.entrySet()) {
            String cidr = entry.getKey();
            if (RemotingHelper.DEFAULT_CIDR_ALL.equals(cidr) || RemotingHelper.ipInCIDR(hostAndPort[0], cidr)) {
                return entry;
            }
        }
        return null;
    }

    private Bootstrap fetchBootstrap(String addr) {
        Map.Entry<String, SocksProxyConfig> proxyEntry = getProxy(addr);
        if (proxyEntry == null) {
            return bootstrap;
        }

        String cidr = proxyEntry.getKey();
        SocksProxyConfig socksProxyConfig = proxyEntry.getValue();

        LOGGER.info("Netty fetch bootstrap, addr: {}, cidr: {}, proxy: {}",
            addr, cidr, socksProxyConfig != null ? socksProxyConfig.getAddr() : "");

        Bootstrap bootstrapWithProxy = bootstrapMap.get(cidr);
        if (bootstrapWithProxy == null) {
            bootstrapWithProxy = createBootstrap(socksProxyConfig);
            Bootstrap old = bootstrapMap.putIfAbsent(cidr, bootstrapWithProxy);
            if (old != null) {
                bootstrapWithProxy = old;
            }
        }
        return bootstrapWithProxy;
    }

    private Bootstrap createBootstrap(final SocksProxyConfig proxy) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
            .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
            .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();
                    if (nettyClientConfig.isUseTLS()) {
                        if (null != sslContext) {
                            pipeline.addFirst(defaultEventExecutorGroup,
                                "sslHandler", sslContext.newHandler(ch.alloc()));
                            LOGGER.info("Prepend SSL handler");
                        } else {
                            LOGGER.warn("Connections are insecure as SSLContext is null!");
                        }
                    }

                    // Netty Socks5 Proxy
                    if (proxy != null) {
                        String[] hostAndPort = getHostAndPort(proxy.getAddr());
                        pipeline.addFirst(new Socks5ProxyHandler(
                            new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])),
                            proxy.getUsername(), proxy.getPassword()));
                    }

                    pipeline.addLast(
                        nettyClientConfig.isDisableNettyWorkerGroup() ? null : defaultEventExecutorGroup,
                        new NettyEncoder(),
                        new NettyDecoder(),
                        new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                        new NettyConnectManageHandler(),
                        new NettyClientHandler());
                }
            });

        // Support Netty Socks5 Proxy
        if (proxy != null) {
            bootstrap.resolver(NoopAddressResolverGroup.INSTANCE);
        }
        return bootstrap;
    }

    // Do not use RemotingUtil, it will directly resolve the domain
    private String[] getHostAndPort(String address) {
        return address.split(":");
    }

    @Override
    public void shutdown() {
        try {
            this.timer.stop();

            for (String addr : this.channelTables.keySet()) {
                this.closeChannel(addr, this.channelTables.get(addr).getChannel());
            }

            this.channelTables.clear();

            this.eventLoopGroupWorker.shutdownGracefully();

            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            LOGGER.error("NettyRemotingClient shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                LOGGER.error("NettyRemotingServer shutdown exception, ", e);
            }
        }

        if (this.scanExecutor != null) {
            try {
                this.scanExecutor.shutdown();
            } catch (Exception e) {
                LOGGER.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    public void closeChannel(final String addr, final Channel channel) {
        if (null == channel) {
            return;
        }

        final String addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTables.get(addrRemote);

                    LOGGER.info("closeChannel: begin close the channel[{}] Found: {}", addrRemote, prevCW != null);

                    if (null == prevCW) {
                        LOGGER.info("closeChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    } else if (prevCW.getChannel() != channel) {
                        LOGGER.info("closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.",
                            addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        LOGGER.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                    }

                    RemotingHelper.closeChannel(channel);
                } catch (Exception e) {
                    LOGGER.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                LOGGER.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOGGER.error("closeChannel exception", e);
        }
    }

    public void closeChannel(final Channel channel) {
        if (null == channel) {
            return;
        }

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    String addrRemote = null;
                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {
                        String key = entry.getKey();
                        ChannelWrapper prev = entry.getValue();
                        if (prev.getChannel() != null) {
                            if (prev.getChannel() == channel) {
                                prevCW = prev;
                                addrRemote = key;
                                break;
                            }
                        }
                    }

                    if (null == prevCW) {
                        LOGGER.info("eventCloseChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        LOGGER.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        RemotingHelper.closeChannel(channel);
                    }
                } catch (Exception e) {
                    LOGGER.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                LOGGER.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOGGER.error("closeChannel exception", e);
        }
    }

    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if (!addrs.isEmpty()) {
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (String addr : addrs) {
                    if (!old.contains(addr)) {
                        update = true;
                        break;
                    }
                }
            }

            if (update) {
                Collections.shuffle(addrs);
                LOGGER.info("name server address updated. NEW : {} , OLD: {}", addrs, old);
                this.namesrvAddrList.set(addrs);

                // should close the channel if choosed addr is not exist.
                if (this.namesrvAddrChoosed.get() != null && !addrs.contains(this.namesrvAddrChoosed.get())) {
                    String namesrvAddr = this.namesrvAddrChoosed.get();
                    for (String addr : this.channelTables.keySet()) {
                        if (addr.contains(namesrvAddr)) {
                            ChannelWrapper channelWrapper = this.channelTables.get(addr);
                            if (channelWrapper != null) {
                                closeChannel(channelWrapper.getChannel());
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * NettyRemotingClient的方法
     * <p>
     * 同步调用
     */
    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        long beginStartTime = System.currentTimeMillis();
        //根据addr建立连接，获取channel，随机选择nameServer连接的逻辑就在这里
        final Channel channel = this.getAndCreateChannel(addr);
        String channelRemoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
        if (channel != null && channel.isActive()) {
            try {
                //执行rpc钩子的doBeforeRequest方法
                doBeforeRpcHooks(channelRemoteAddr, request);
                //检查超时，如果超时则抛出异常
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new RemotingTimeoutException("invokeSync call the addr[" + channelRemoteAddr + "] timeout");
                }
                //执行同步远程调用，获得调用结果
                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis - costTime);
                //执行rpc钩子的doAfterResponse方法
                doAfterRpcHooks(channelRemoteAddr, request, response);
                this.updateChannelLastResponseTime(addr);
                return response;
            } catch (RemotingSendRequestException e) {
                LOGGER.warn("invokeSync: send request exception, so close the channel[{}]", channelRemoteAddr);
                this.closeChannel(addr, channel);
                throw e;
            } catch (RemotingTimeoutException e) {
                if (nettyClientConfig.isClientCloseSocketIfTimeout()) {
                    this.closeChannel(addr, channel);
                    LOGGER.warn("invokeSync: close socket because of timeout, {}ms, {}", timeoutMillis, channelRemoteAddr);
                }
                LOGGER.warn("invokeSync: wait response timeout exception, the channel[{}]", channelRemoteAddr);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void closeChannels(List<String> addrList) {
        for (String addr : addrList) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw == null) {
                continue;
            }
            this.closeChannel(addr, cw.getChannel());
        }
        interruptPullRequests(new HashSet<>(addrList));
    }

    private void interruptPullRequests(Set<String> brokerAddrSet) {
        for (ResponseFuture responseFuture : responseTable.values()) {
            RemotingCommand cmd = responseFuture.getRequestCommand();
            if (cmd == null) {
                continue;
            }
            String remoteAddr = RemotingHelper.parseChannelRemoteAddr(responseFuture.getChannel());
            // interrupt only pull message request
            if (brokerAddrSet.contains(remoteAddr) && (cmd.getCode() == 11 || cmd.getCode() == 361)) {
                LOGGER.info("interrupt {}", cmd);
                responseFuture.interrupt();
            }
        }
    }

    private void updateChannelLastResponseTime(final String addr) {
        String address = addr;
        if (address == null) {
            address = this.namesrvAddrChoosed.get();
        }
        if (address == null) {
            LOGGER.warn("[updateChannelLastResponseTime] could not find address!!");
            return;
        }
        ChannelWrapper channelWrapper = this.channelTables.get(address);
        if (channelWrapper != null && channelWrapper.isOK()) {
            channelWrapper.updateLastResponseTime();
        }
    }

    /**
     * NettyRemotingClient的方法
     * <p>
     * 获取并建立连接
     *
     * @param addr 地址
     */
    private Channel getAndCreateChannel(final String addr) throws InterruptedException {
        /*
         * 如果addr为null，则获取并且创建nameServer长连接
         */
        if (null == addr) {
            return getAndCreateNameserverChannel();
        }
        /*
         * 否则，尝试从缓存中获取长连接，获取不到才会创建
         */
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }

        return this.createChannel(addr);
    }

    /**
     * NettyRemotingClient的方法
     * 获取并建立nameServer连接
     */
    private Channel getAndCreateNameserverChannel() throws InterruptedException {
        //如果此前已经选择了nameServer，并且通道存在且是激活状态，那么直接获取该nameServer的通道
        //即通道可以被不同的consumer和producer复用
        String addr = this.namesrvAddrChoosed.get();
        if (addr != null) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw != null && cw.isOK()) {
                return cw.getChannel();
            }
        }

        final List<String> addrList = this.namesrvAddrList.get();
        //加锁
        if (this.namesrvChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            //加锁之后再次判断，防止并发
            try {
                addr = this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper cw = this.channelTables.get(addr);
                    if (cw != null && cw.isOK()) {
                        return cw.getChannel();
                    }
                }
                /*
                 * 选择一个nameServer建立长连接
                 */
                if (addrList != null && !addrList.isEmpty()) {
                    for (int i = 0; i < addrList.size(); i++) {
                        //获取下一个nameServer的索引，初始化数据是随机生成的
                        int index = this.namesrvIndex.incrementAndGet();
                        //取绝对值
                        index = Math.abs(index);
                        //对nameServer集合长度取余数
                        index = index % addrList.size();
                        //获取最终确定的nameServer地址
                        String newAddr = addrList.get(index);
                        //设置到被选择的namesrvAddrChoosed属性中
                        this.namesrvAddrChoosed.set(newAddr);
                        LOGGER.info("new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}", addr, newAddr, namesrvIndex);
                        //针对被选择的地址建立长连接
                        Channel channelNew = this.createChannel(newAddr);
                        //如果建立连接成功就返回，否则集训循环选择下一个nameServer地址创建
                        if (channelNew != null) {
                            return channelNew;
                        }
                    }
                    //没有一个nameServer地址建立成功，那么抛出异常
                    throw new RemotingConnectException(addrList.toString());
                }
            } catch (Exception e) {
                LOGGER.error("getAndCreateNameserverChannel: create name server channel exception", e);
            } finally {
                //解锁
                this.namesrvChannelLock.unlock();
            }
        } else {
            LOGGER.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    private Channel createChannel(final String addr) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }

        if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                boolean createNewConnection;
                cw = this.channelTables.get(addr);
                if (cw != null) {

                    if (cw.isOK()) {
                        return cw.getChannel();
                    } else if (!cw.getChannelFuture().isDone()) {
                        createNewConnection = false;
                    } else {
                        this.channelTables.remove(addr);
                        createNewConnection = true;
                    }
                } else {
                    createNewConnection = true;
                }

                if (createNewConnection) {
                    String[] hostAndPort = getHostAndPort(addr);
                    ChannelFuture channelFuture = fetchBootstrap(addr)
                        .connect(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
                    LOGGER.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                    cw = new ChannelWrapper(channelFuture);
                    this.channelTables.put(addr, cw);
                }
            } catch (Exception e) {
                LOGGER.error("createChannel: create channel exception", e);
            } finally {
                this.lockChannelTables.unlock();
            }
        } else {
            LOGGER.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        if (cw != null) {
            ChannelFuture channelFuture = cw.getChannelFuture();
            if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                if (cw.isOK()) {
                    LOGGER.info("createChannel: connect remote host[{}] success, {}", addr, channelFuture.toString());
                    return cw.getChannel();
                } else {
                    LOGGER.warn("createChannel: connect remote host[" + addr + "] failed, " + channelFuture.toString());
                }
            } else {
                LOGGER.warn("createChannel: connect remote host[{}] timeout {}ms, {}", addr, this.nettyClientConfig.getConnectTimeoutMillis(),
                    channelFuture.toString());
            }
        }

        return null;
    }

    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException {
        //开始时间
        long beginStartTime = System.currentTimeMillis();
        //获取或创建一个与broker的连接
        final Channel channel = this.getAndCreateChannel(addr);
        String channelRemoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
        if (channel != null && channel.isActive()) {
            try {
                //执行rpc钩子的doBeforeRequest方法
                doBeforeRpcHooks(channelRemoteAddr, request);
                long costTime = System.currentTimeMillis() - beginStartTime;
                //如果超时，则直接抛出异常，不再执行
                if (timeoutMillis < costTime) {
                    throw new RemotingTooMuchRequestException("invokeAsync call the addr[" + channelRemoteAddr + "] timeout");
                }
                //执行异步远程调用
                this.invokeAsyncImpl(channel, request, timeoutMillis - costTime, new InvokeCallbackWrapper(invokeCallback, addr));
            } catch (RemotingSendRequestException e) {
                LOGGER.warn("invokeAsync: send request exception, so close the channel[{}]", channelRemoteAddr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * NettyRemotingClient的方法
     * <p>
     * 单向消息发送的通用方法
     *
     * @param addr          服务器地址
     * @param request       请求命令对象
     * @param timeoutMillis 超时时间
     */
    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        //获取或者建立同服务器的通道，即连接
        final Channel channel = this.getAndCreateChannel(addr);
        String channelRemoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
        if (channel != null && channel.isActive()) {
            try {
                //执行rpcHook的前置方法doBeforeRequest
                doBeforeRpcHooks(channelRemoteAddr, request);
                /*
                 * 调用另一个invokeOnewayImpl方法，发送单向消息
                 */
                this.invokeOnewayImpl(channel, request, timeoutMillis);
            } catch (RemotingSendRequestException e) {
                LOGGER.warn("invokeOneway: send request exception, so close the channel[{}]", channelRemoteAddr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(channelRemoteAddr);
        }
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public boolean isChannelWritable(String addr) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.isWritable();
        }
        return true;
    }

    @Override
    public boolean isAddressReachable(String addr) {
        if (addr == null || addr.isEmpty()) {
            return false;
        }
        try {
            Channel channel = getAndCreateChannel(addr);
            return channel != null && channel.isActive();
        } catch (Exception e) {
            LOGGER.warn("Get and create channel of {} failed", addr, e);
            return false;
        }
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public List<String> getAvailableNameSrvList() {
        return new ArrayList<>(this.availableNamesrvAddrMap.keySet());
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        if (nettyClientConfig.isDisableCallbackExecutor()) {
            return null;
        }
        return callbackExecutor != null ? callbackExecutor : publicExecutor;
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    protected void scanChannelTablesOfNameServer() {
        List<String> nameServerList = this.namesrvAddrList.get();
        if (nameServerList == null) {
            LOGGER.warn("[SCAN] Addresses of name server is empty!");
            return;
        }

        for (String addr : this.channelTables.keySet()) {
            ChannelWrapper channelWrapper = this.channelTables.get(addr);
            if (channelWrapper == null) {
                continue;
            }

            if ((System.currentTimeMillis() - channelWrapper.getLastResponseTime()) > this.nettyClientConfig.getChannelNotActiveInterval()) {
                LOGGER.warn("[SCAN] No response after {} from name server {}, so close it!", channelWrapper.getLastResponseTime(),
                    addr);
                closeChannel(addr, channelWrapper.getChannel());
            }
        }
    }

    private void scanAvailableNameSrv() {
        List<String> nameServerList = this.namesrvAddrList.get();
        if (nameServerList == null) {
            LOGGER.debug("scanAvailableNameSrv addresses of name server is null!");
            return;
        }

        for (String address : NettyRemotingClient.this.availableNamesrvAddrMap.keySet()) {
            if (!nameServerList.contains(address)) {
                LOGGER.warn("scanAvailableNameSrv remove invalid address {}", address);
                NettyRemotingClient.this.availableNamesrvAddrMap.remove(address);
            }
        }

        for (final String namesrvAddr : nameServerList) {
            scanExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Channel channel = NettyRemotingClient.this.getAndCreateChannel(namesrvAddr);
                        if (channel != null) {
                            NettyRemotingClient.this.availableNamesrvAddrMap.putIfAbsent(namesrvAddr, true);
                        } else {
                            Boolean value = NettyRemotingClient.this.availableNamesrvAddrMap.remove(namesrvAddr);
                            if (value != null) {
                                LOGGER.warn("scanAvailableNameSrv remove unconnected address {}", namesrvAddr);
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error("scanAvailableNameSrv get channel of {} failed, ", namesrvAddr, e);
                    }
                }
            });
        }
    }

    static class ChannelWrapper {
        private final ChannelFuture channelFuture;
        // only affected by sync or async request, oneway is not included.
        private long lastResponseTime;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
            this.lastResponseTime = System.currentTimeMillis();
        }

        public boolean isOK() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        public boolean isWritable() {
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }

        public long getLastResponseTime() {
            return this.lastResponseTime;
        }

        public void updateLastResponseTime() {
            this.lastResponseTime = System.currentTimeMillis();
        }
    }

    class InvokeCallbackWrapper implements InvokeCallback {

        private final InvokeCallback invokeCallback;
        private final String addr;

        public InvokeCallbackWrapper(InvokeCallback invokeCallback, String addr) {
            this.invokeCallback = invokeCallback;
            this.addr = addr;
        }

        @Override
        public void operationComplete(ResponseFuture responseFuture) {
            if (responseFuture != null && responseFuture.isSendRequestOK() && responseFuture.getResponseCommand() != null) {
                NettyRemotingClient.this.updateChannelLastResponseTime(addr);
            }
            this.invokeCallback.operationComplete(responseFuture);
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        /**
         * 处理来自服务端的RemotingCommand消息
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            LOGGER.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.channel()));
            }
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, promise);
            NettyRemotingClient.this.failFast(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    LOGGER.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this
                            .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            LOGGER.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }
        }
    }
}
