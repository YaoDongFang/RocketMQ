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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

@SuppressWarnings("NullableProblems")
public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);
    private static final Logger TRAFFIC_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_TRAFFIC_NAME);

    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGroupSelector;
    private final EventLoopGroup eventLoopGroupBoss;
    private final NettyServerConfig nettyServerConfig;

    private final ExecutorService publicExecutor;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ChannelEventListener channelEventListener;

    private final HashedWheelTimer timer = new HashedWheelTimer(r -> new Thread(r, "ServerHouseKeepingService"));

    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    /**
     * NettyRemotingServer may hold multiple SubRemotingServer, each server will be stored in this container with a
     * ListenPort key.
     */
    private final ConcurrentMap<Integer/*Port*/, NettyRemotingAbstract> remotingServerTable = new ConcurrentHashMap<>();

    public static final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";
    public static final String TLS_HANDLER_NAME = "sslHandler";
    public static final String FILE_REGION_ENCODER_NAME = "fileRegionEncoder";

    // sharable handlers
    private HandshakeHandler handshakeHandler;
    private NettyEncoder encoder;
    private NettyConnectManageHandler connectionManageHandler;
    private NettyServerHandler serverHandler;
    private RemotingCodeDistributionHandler distributionHandler;

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig,
        final ChannelEventListener channelEventListener) {
        //设置服务器单向、异步发送信号量
        super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());
        //创建Netty服务端启动类，引导启动服务端
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;

        //服务器回调执行线程数量，默认设置为4
        this.publicExecutor = buildPublicExecutor(nettyServerConfig);
        //创建一个公共线程池，负责处理某些请求业务，例如发送异步消息回调，线程名以NettyServerPublicExecutor_为前缀
        this.scheduledExecutorService = buildScheduleExecutor();

        // epoll设置
        this.eventLoopGroupBoss = buildBossEventLoopGroup();
        this.eventLoopGroupSelector = buildEventLoopGroupSelector();

        //加载ssl信息
        loadSslContext();
    }

    private EventLoopGroup buildEventLoopGroupSelector() {
        if (useEpoll()) {
            return new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactoryImpl("NettyServerEPOLLSelector_"));
        } else {
            //Worker EventLoopGroup 默认3个线程，线程名以NettyServerNIOSelector_为前缀
            return new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactoryImpl("NettyServerNIOSelector_"));
        }
    }

    private EventLoopGroup buildBossEventLoopGroup() {
        /*
         * 是否使用epoll模型，并且初始化Boss EventLoopGroup和Worker EventLoopGroup这两个事件循环组
         * 如果是linux内核，并且指定开启epoll，并且系统支持epoll，才会使用EpollEventLoopGroup，否则使用NioEventLoopGroup
         */
        if (useEpoll()) {
            /*采用了epoll*/
            return new EpollEventLoopGroup(1, new ThreadFactoryImpl("NettyEPOLLBoss_"));
        } else {
            /*未采用epoll*/
            //Boss EventLoopGroup 默认1个线程，线程名以NettyNIOBoss_为前缀
            return new NioEventLoopGroup(1, new ThreadFactoryImpl("NettyNIOBoss_"));
        }
    }

    private ExecutorService buildPublicExecutor(NettyServerConfig nettyServerConfig) {
        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        return Executors.newFixedThreadPool(publicThreadNums, new ThreadFactoryImpl("NettyServerPublicExecutor_"));
    }

    private ScheduledExecutorService buildScheduleExecutor() {
        return new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryImpl("NettyServerScheduler_", true),
            new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    public void loadSslContext() {
        TlsMode tlsMode = TlsSystemConfig.tlsMode;
        log.info("Server is running in TLS {} mode", tlsMode.getName());

        if (tlsMode != TlsMode.DISABLED) {
            try {
                sslContext = TlsHelper.buildSslContext(false);
                log.info("SSLContext created for server");
            } catch (CertificateException | IOException e) {
                log.error("Failed to create SSLContext for server", e);
            }
        }
    }

    private boolean useEpoll() {
        return NetworkUtil.isLinuxPlatform()
            && nettyServerConfig.isUseEpollNativeSelector()
            && Epoll.isAvailable();
    }

    /**
     * Rocketmq的远程通信是基于Netty的，从start方法中可以明显的看出来典型的netty服务的启动流程，
     * 通过serverBootstrap的一系列方法帮助设置启动配置，
     * 包括配置parentGroup和childGroup，配置IO模型，配置连接属性，配置服务端口固定为9876，
     * 配置用于执行在真正执行业务逻辑之前需要进行的SSL验证、编解码、空闲检查、网络连接管理等操作的defaultEventExecutorGroup，
     * 配置真正处理请求的serverHandler等等。
     * 主要看几个handler
     */
    @Override
    public void start() {
        /*
         * 1 创建默认事件处理器组，线程数默认8个线程，线程名以NettyServerCodecThread_为前缀。
         * 主要用于执行在真正执行业务逻辑之前需要进行的SSL验证、编解码、空闲检查、网络连接管理等操作
         * 其工作时间位于IO线程组之后，process线程组之前
         */
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyServerConfig.getServerWorkerThreads(),
            new ThreadFactoryImpl("NettyServerCodecThread_"));

        /*
         * 2 准备一些共享handler
         * 包括handshakeHandler、encoder、connectionManageHandler、serverHandler
         */
        prepareSharableHandlers();
        /*
         * 3 配置NettyServer的启动参数
         * 包括handshakeHandler、encoder、connectionManageHandler、serverHandler
         */
        //配置bossGroup为此前创建的eventLoopGroupBoss，默认1个线程，用于处理连接时间
        //配置workerGroup为此前创建的eventLoopGroupSelector，默认三个线程，用于处理IO事件
        serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                //IO模型
            .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
            /*设置通道的选项参数， 对于服务端而言就是ServerSocketChannel， 客户端而言就是SocketChannel*/
            /*option主要是针对boss线程组，child主要是针对worker线程组*/
            //对应的是tcp/ip协议listen函数中的backlog参数
            .option(ChannelOption.SO_BACKLOG, 1024)
                //对应于套接字选项中的SO_REUSEADDR，这个参数表示允许重复使用本地地址和端口
            .option(ChannelOption.SO_REUSEADDR, true)
                //对应于套接字选项中的SO_KEEPALIVE，该参数用于设置TCP连接，当设置该选项以后，连接会测试链接的状态
            .childOption(ChannelOption.SO_KEEPALIVE, false)
                //对应于套接字选项中的TCP_NODELAY，该参数的使用与Nagle算法有关
            .childOption(ChannelOption.TCP_NODELAY, true)
                //配置本地地址，监听端口为此前设置的9876
            .localAddress(new InetSocketAddress(this.nettyServerConfig.getBindAddress(),
                this.nettyServerConfig.getListenPort()))
                /*设置用于为 Channel 的请求提供服务的 ChannelHandler*/
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    configChannel(ch);
                }
            });

        addCustomConfig(serverBootstrap);

        try {
            /*
             * 启动Netty服务
             */
            ChannelFuture sync = serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            //设置端口号，默认9876
            if (0 == nettyServerConfig.getListenPort()) {
                this.nettyServerConfig.setListenPort(addr.getPort());
            }
            log.info("RemotingServer started, listening {}:{}", this.nettyServerConfig.getBindAddress(),
                this.nettyServerConfig.getListenPort());
            // 注册
            this.remotingServerTable.put(this.nettyServerConfig.getListenPort(), this);
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Failed to bind to %s:%d", nettyServerConfig.getBindAddress(),
                nettyServerConfig.getListenPort()), e);
        }
        //如果channelEventListener不为null，那么启动netty事件执行器
        //这里的listener就是之前初始化的BrokerHousekeepingService
        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }


        /*
         * 启动定时任务，初始启动3秒后执行，此后每隔1秒执行一次
         * 扫描responseTable，将超时的ResponseFuture直接移除，并且执行这些超时ResponseFuture的回调
         */
        TimerTask timerScanResponseTable = new TimerTask() {
            @Override
            public void run(Timeout timeout) {
                try {
                    NettyRemotingServer.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                } finally {
                    timer.newTimeout(this, 1000, TimeUnit.MILLISECONDS);
                }
            }
        };
        this.timer.newTimeout(timerScanResponseTable, 1000 * 3, TimeUnit.MILLISECONDS);

        /*
         * 启动定时任务，初始启动1秒后执行，此后每隔1秒执行一次
         * 打印出栈，入栈信息
         */
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                NettyRemotingServer.this.printRemotingCodeDistribution();
            } catch (Throwable e) {
                TRAFFIC_LOGGER.error("NettyRemotingServer print remoting code distribution exception", e);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * config channel in ChannelInitializer
     *
     * @param ch the SocketChannel needed to init
     * @return the initialized ChannelPipeline, sub class can use it to extent in the future
     */
    protected ChannelPipeline configChannel(SocketChannel ch) {
        //ChannelPipeline一个ChannelHandler的链表，Netty处理请求基于责任链默认
        //里面的ChannelHandler就是用于处理请求的
        return ch.pipeline()
                //处理TSL协议握手的Handler
                .addLast(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, handshakeHandler)
                //为defaultEventExecutorGroup，添加handler
            .addLast(defaultEventExecutorGroup,
                    //RocketMQ自定义的请求解码器
                encoder,
                    //RocketMQ自定义的请求编码器
                new NettyDecoder(),
                distributionHandler,
                    //Netty自带的心跳管理器，主要是用来检测远端是否存活
                    //即测试端一定时间内未接受到被测试端消息和一定时间内向被测试端发送消息的超时时间为120秒
                new IdleStateHandler(0, 0,
                    nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                    //连接管理器，他负责连接的激活、断开、超时、异常等事件
                connectionManageHandler,
                    //服务请求处理器，处理RemotingCommand消息，即请求和响应的业务处理，并且返回相应的处理结果。这是重点
                    //例如broker注册、producer/consumer获取Broker、Topic信息等请求都是该处理器处理
                    //serverHandler最终会将请求根据不同的消息类型code分发到不同的process线程池处理
                serverHandler
            );
    }

    private void addCustomConfig(ServerBootstrap childHandler) {
        //对应于套接字选项中的SO_SNDBUF，接收缓冲区，默认是65535
        if (nettyServerConfig.getServerSocketSndBufSize() > 0) {
            log.info("server set SO_SNDBUF to {}", nettyServerConfig.getServerSocketSndBufSize());
            childHandler.childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize());
        }
        //对应于套接字选项中的SO_SNDBUF，发送缓冲区，默认是65535
        if (nettyServerConfig.getServerSocketRcvBufSize() > 0) {
            log.info("server set SO_RCVBUF to {}", nettyServerConfig.getServerSocketRcvBufSize());
            childHandler.childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize());
        }
        //用于设置写缓冲区的低水位线和高水位线。
        if (nettyServerConfig.getWriteBufferLowWaterMark() > 0 && nettyServerConfig.getWriteBufferHighWaterMark() > 0) {
            log.info("server set netty WRITE_BUFFER_WATER_MARK to {},{}",
                nettyServerConfig.getWriteBufferLowWaterMark(), nettyServerConfig.getWriteBufferHighWaterMark());
            childHandler.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                nettyServerConfig.getWriteBufferLowWaterMark(), nettyServerConfig.getWriteBufferHighWaterMark()));
        }
        //分配缓冲区
        if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
            childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }
    }

    @Override
    public void shutdown() {
        try {
            this.timer.stop();

            this.eventLoopGroupBoss.shutdownGracefully();

            this.eventLoopGroupSelector.shutdownGracefully();

            this.nettyEventExecutor.shutdown();

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingServer shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
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
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        //defaultRequestProcessor属性
        this.defaultRequestProcessorPair = new Pair<>(processor, executor);
    }

    @Override
    public int localListenPort() {
        return this.nettyServerConfig.getListenPort();
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return processorTable.get(requestCode);
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair() {
        return defaultRequestProcessorPair;
    }

    @Override
    public RemotingServer newRemotingServer(final int port) {
        SubRemotingServer remotingServer = new SubRemotingServer(port,
            this.nettyServerConfig.getServerOnewaySemaphoreValue(),
            this.nettyServerConfig.getServerAsyncSemaphoreValue());
        NettyRemotingAbstract existingServer = this.remotingServerTable.putIfAbsent(port, remotingServer);
        if (existingServer != null) {
            throw new RuntimeException("The port " + port + " already in use by another RemotingServer");
        }
        return remotingServer;
    }

    @Override
    public void removeRemotingServer(final int port) {
        this.remotingServerTable.remove(port);
    }

    @Override
    public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        return this.invokeSyncImpl(channel, request, timeoutMillis);
    }

    @Override
    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
    }

    @Override
    public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeOnewayImpl(channel, request, timeoutMillis);
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    private void prepareSharableHandlers() {
        // 这是用来处理TSL协议握手的Handler，无需过多关注。
        handshakeHandler = new HandshakeHandler(TlsSystemConfig.tlsMode);
        // 这是RocketMQ自定义的请求解码和编码器，处理报文的编解码操作。负责网络传输数据和 RemotingCommand 之间的编解码。
        encoder = new NettyEncoder();
        // 处理连接事件的handler，负责连接的激活、断开、超时、异常等事件。
        connectionManageHandler = new NettyConnectManageHandler();
        // 处理读写事件的handler，简单的说真正处理业务请求的，这是重点，后面会在专门学习通信的时候解析。
        serverHandler = new NettyServerHandler();
        // 入站 出战
        distributionHandler = new RemotingCodeDistributionHandler();
    }

    private void printRemotingCodeDistribution() {
        if (distributionHandler != null) {
            String inBoundSnapshotString = distributionHandler.getInBoundSnapshotString();
            if (inBoundSnapshotString != null) {
                TRAFFIC_LOGGER.info("Port: {}, RequestCode Distribution: {}",
                    nettyServerConfig.getListenPort(), inBoundSnapshotString);
            }

            String outBoundSnapshotString = distributionHandler.getOutBoundSnapshotString();
            if (outBoundSnapshotString != null) {
                TRAFFIC_LOGGER.info("Port: {}, ResponseCode Distribution: {}",
                    nettyServerConfig.getListenPort(), outBoundSnapshotString);
            }
        }
    }

    public DefaultEventExecutorGroup getDefaultEventExecutorGroup() {
        return defaultEventExecutorGroup;
    }

    public HandshakeHandler getHandshakeHandler() {
        return handshakeHandler;
    }

    public NettyEncoder getEncoder() {
        return encoder;
    }

    public NettyConnectManageHandler getConnectionManageHandler() {
        return connectionManageHandler;
    }

    public NettyServerHandler getServerHandler() {
        return serverHandler;
    }

    public RemotingCodeDistributionHandler getDistributionHandler() {
        return distributionHandler;
    }

    @ChannelHandler.Sharable
    public class HandshakeHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private final TlsMode tlsMode;

        private static final byte HANDSHAKE_MAGIC_CODE = 0x16;

        HandshakeHandler(TlsMode tlsMode) {
            this.tlsMode = tlsMode;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {

            // Peek the first byte to determine if the content is starting with TLS handshake
            byte b = msg.getByte(0);

            if (b == HANDSHAKE_MAGIC_CODE) {
                switch (tlsMode) {
                    case DISABLED:
                        ctx.close();
                        log.warn("Clients intend to establish an SSL connection while this server is running in SSL disabled mode");
                        break;
                    case PERMISSIVE:
                    case ENFORCING:
                        if (null != sslContext) {
                            ctx.pipeline()
                                .addAfter(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, TLS_HANDLER_NAME, sslContext.newHandler(ctx.channel().alloc()))
                                .addAfter(defaultEventExecutorGroup, TLS_HANDLER_NAME, FILE_REGION_ENCODER_NAME, new FileRegionEncoder());
                            log.info("Handlers prepended to channel pipeline to establish SSL connection");
                        } else {
                            ctx.close();
                            log.error("Trying to establish an SSL connection but sslContext is null");
                        }
                        break;

                    default:
                        log.warn("Unknown TLS mode");
                        break;
                }
            } else if (tlsMode == TlsMode.ENFORCING) {
                ctx.close();
                log.warn("Clients intend to establish an insecure connection while this server is running in SSL enforcing mode");
            }

            try {
                // Remove this handler
                ctx.pipeline().remove(this);
            } catch (NoSuchElementException e) {
                log.error("Error while removing HandshakeHandler", e);
            }

            // Hand over this message to the next .
            ctx.fireChannelRead(msg.retain());
        }
    }

    @ChannelHandler.Sharable
    public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
            int localPort = RemotingHelper.parseSocketAddressPort(ctx.channel().localAddress());
            NettyRemotingAbstract remotingAbstract = NettyRemotingServer.this.remotingServerTable.get(localPort);
            if (localPort != -1 && remotingAbstract != null) {
                remotingAbstract.processMessageReceived(ctx, msg);
                return;
            }
            // The related remoting server has been shutdown, so close the connected channel
            RemotingHelper.closeChannel(ctx.channel());
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            Channel channel = ctx.channel();
            if (channel.isWritable()) {
                if (!channel.config().isAutoRead()) {
                    channel.config().setAutoRead(true);
                    log.info("Channel[{}] turns writable, bytes to buffer before changing channel to un-writable: {}",
                        RemotingHelper.parseChannelRemoteAddr(channel), channel.bytesBeforeUnwritable());
                }
            } else {
                channel.config().setAutoRead(false);
                log.warn("Channel[{}] auto-read is disabled, bytes to drain before it turns writable: {}",
                    RemotingHelper.parseChannelRemoteAddr(channel), channel.bytesBeforeWritable());
            }
            super.channelWritabilityChanged(ctx);
        }
    }

    @ChannelHandler.Sharable
    public class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelRegistered {}", remoteAddress);
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddress);
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]", remoteAddress);
            super.channelActive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]", remoteAddress);
            super.channelInactive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]", remoteAddress);
                    RemotingHelper.closeChannel(ctx.channel());
                    if (NettyRemotingServer.this.channelEventListener != null) {
                        NettyRemotingServer.this
                            .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }

            RemotingHelper.closeChannel(ctx.channel());
        }
    }

    /**
     * The NettyRemotingServer supports bind multiple ports, each port bound by a SubRemotingServer. The
     * SubRemotingServer will delegate all the functions to NettyRemotingServer, so the sub server can share all the
     * resources from its parent server.
     */
    class SubRemotingServer extends NettyRemotingAbstract implements RemotingServer {
        private volatile int listenPort;
        private volatile Channel serverChannel;

        SubRemotingServer(final int port, final int permitsOnway, final int permitsAsync) {
            super(permitsOnway, permitsAsync);
            listenPort = port;
        }

        @Override
        public void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
            final ExecutorService executor) {
            ExecutorService executorThis = executor;
            if (null == executor) {
                executorThis = NettyRemotingServer.this.publicExecutor;
            }

            Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
            this.processorTable.put(requestCode, pair);
        }

        @Override
        public void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor) {
            this.defaultRequestProcessorPair = new Pair<>(processor, executor);
        }

        @Override
        public int localListenPort() {
            return listenPort;
        }

        @Override
        public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode) {
            return this.processorTable.get(requestCode);
        }

        @Override
        public Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair() {
            return this.defaultRequestProcessorPair;
        }

        @Override
        public RemotingServer newRemotingServer(final int port) {
            throw new UnsupportedOperationException("The SubRemotingServer of NettyRemotingServer " +
                "doesn't support new nested RemotingServer");
        }

        @Override
        public void removeRemotingServer(final int port) {
            throw new UnsupportedOperationException("The SubRemotingServer of NettyRemotingServer " +
                "doesn't support remove nested RemotingServer");
        }

        @Override
        public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request,
            final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
            return this.invokeSyncImpl(channel, request, timeoutMillis);
        }

        @Override
        public void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
            final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
            this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
        }

        @Override
        public void invokeOneway(final Channel channel, final RemotingCommand request,
            final long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
            this.invokeOnewayImpl(channel, request, timeoutMillis);
        }

        @Override
        public void start() {
            try {
                if (listenPort < 0) {
                    listenPort = 0;
                }
                this.serverChannel = NettyRemotingServer.this.serverBootstrap.bind(listenPort).sync().channel();
                if (0 == listenPort) {
                    InetSocketAddress addr = (InetSocketAddress) this.serverChannel.localAddress();
                    this.listenPort = addr.getPort();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("this.subRemotingServer.serverBootstrap.bind().sync() InterruptedException", e);
            }
        }

        @Override
        public void shutdown() {
            if (this.serverChannel != null) {
                try {
                    this.serverChannel.close().await(5, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
            }
        }

        @Override
        public ChannelEventListener getChannelEventListener() {
            return NettyRemotingServer.this.getChannelEventListener();
        }

        @Override
        public ExecutorService getCallbackExecutor() {
            return NettyRemotingServer.this.getCallbackExecutor();
        }
    }
}
