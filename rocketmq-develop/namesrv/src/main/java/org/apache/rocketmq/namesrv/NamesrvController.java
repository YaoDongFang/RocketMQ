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
package org.apache.rocketmq.namesrv;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClientRequestProcessor;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.route.ZoneRouteRPCHook;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.srvutil.FileWatchService;

public class NamesrvController {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private static final Logger WATER_MARK_LOG = LoggerFactory.getLogger(LoggerName.NAMESRV_WATER_MARK_LOGGER_NAME);

    private final NamesrvConfig namesrvConfig;

    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;

    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
            new BasicThreadFactory.Builder().namingPattern("NSScheduledThread").daemon(true).build());

    private final ScheduledExecutorService scanExecutorService = new ScheduledThreadPoolExecutor(1,
            new BasicThreadFactory.Builder().namingPattern("NSScanScheduledThread").daemon(true).build());

    private final KVConfigManager kvConfigManager;
    private final RouteInfoManager routeInfoManager;

    private RemotingClient remotingClient;
    private RemotingServer remotingServer;

    private final BrokerHousekeepingService brokerHousekeepingService;

    private ExecutorService defaultExecutor;
    private ExecutorService clientRequestExecutor;

    private BlockingQueue<Runnable> defaultThreadPoolQueue;
    private BlockingQueue<Runnable> clientRequestThreadPoolQueue;

    private final Configuration configuration;
    private FileWatchService fileWatchService;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this(namesrvConfig, nettyServerConfig, new NettyClientConfig());
    }

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig) {
        //nameserver的配置
        this.namesrvConfig = namesrvConfig;
        //nameserver的netty服务的配置
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        //kv配置管理器
        this.kvConfigManager = new KVConfigManager(this);
        //Broker连接的各种事件的处理服务，是处理Broker连接发生变化的服务
        //主要用于监听在Channel通道关闭事件触发时调用RouteInfoManager#onChannelDestroy清除路由信息
        // 进入
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        //路由信息管理器
        this.routeInfoManager = new RouteInfoManager(namesrvConfig, this);
        //配置类，并将namesrvConfig和nettyServerConfig的配置注册到内部的allConfigs集合中
        this.configuration = new Configuration(LOGGER, this.namesrvConfig, this.nettyServerConfig);
        //存储路径配置
        this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
    }

    public boolean initialize() {
        /*
         * 1 加载KV配置并存储到kvConfigManager内部的configTable属性中
         * KVConfig配置文件默认路径是 ${user.home}/namesrv/kvConfig.json
         */
        loadConfig();
        /*
         * netty相关
         */
        initiateNetworkComponents();
        /*
         * 创建netty远程通信执行器线程池
         */
        initiateThreadExecutors();
        /*
         * 4 注册默认请求处理器DefaultRequestProcessor
         * 将remotingExecutor绑定到DefaultRequestProcessor上，用作默认的请求处理线程池
         * DefaultRequestProcessor绑定到remotingServer的defaultRequestProcessor属性上
         */
        registerProcessor();
        /*
         * 5 启动一个定时任务
         * 首次启动延迟5秒执行，此后每隔10秒执行一次扫描无效的Broker，并清除Broker相关路由信息的任务
         */
        startScheduleService();
        /*
         * Tls传输相关配置，通信安全的文件监听模块，用来观察网络加密配置文件的更改
         */
        initiateSslContext();
        /*
         * 目前只注册了一个ZoneRouteRPCHook，主要用于区域路由。
         */
        initiateRpcHooks();
        return true;
    }

    private void loadConfig() {
        this.kvConfigManager.load();
    }

    private void startScheduleService() {
        /*
         * 5 启动一个定时任务
         * 首次启动延迟5秒执行，此后每隔10秒执行一次扫描无效的Broker，并清除Broker相关路由信息的任务
         */
        this.scanExecutorService.scheduleAtFixedRate(NamesrvController.this.routeInfoManager::scanNotActiveBroker,
            5, this.namesrvConfig.getScanNotActiveBrokerInterval(), TimeUnit.MILLISECONDS);
        /*
         * 6 启动一个定时任务
         * 首次启动延迟1分钟执行，此后每隔10分钟执行一次打印kv配置信息的任务
         */
        this.scheduledExecutorService.scheduleAtFixedRate(NamesrvController.this.kvConfigManager::printAllPeriodically,
            1, 10, TimeUnit.MINUTES);
        /*
         * 7 启动一个定时任务
         * 首次启动延迟10s执行，此后每隔1s执行一次打印ClientQueueSize:{} ClientQueueSlowTime:{} DefaultQueueSize:{} DefaultQueueSlowTime"
         */
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                NamesrvController.this.printWaterMark();
            } catch (Throwable e) {
                LOGGER.error("printWaterMark error.", e);
            }
        }, 10, 1, TimeUnit.SECONDS);
    }

    private void initiateNetworkComponents() {
        /*
         * 2 创建NameServer的netty远程服务
         * 设置了一个ChannelEventListener，为此前创建brokerHousekeepingService
         * remotingServer是一个基于Netty的用于NameServer与Broker、Consumer、Producer进行网络通信的服务端
         */
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);

        this.remotingClient = new NettyRemotingClient(this.nettyClientConfig);
    }

    private void initiateThreadExecutors() {
        /*
         * 根据配置创建netty server线程池的LinkedBlockingQueue
         */
        this.defaultThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getDefaultThreadPoolQueueCapacity());
        /*
         * 创建netty远程通信执行器线程池，用作默认的请求处理线程池，线程名以RemotingExecutorThread_为前缀
         */
        this.defaultExecutor = new ThreadPoolExecutor(this.namesrvConfig.getDefaultThreadPoolNums(), this.namesrvConfig.getDefaultThreadPoolNums(), 1000 * 60, TimeUnit.MILLISECONDS, this.defaultThreadPoolQueue, new ThreadFactoryImpl("RemotingExecutorThread_")) {
            @Override
            protected <T> RunnableFuture<T> newTaskFor(final Runnable runnable, final T value) {
                return new FutureTaskExt<>(runnable, value);
            }
        };
        /*
         * 根据配置创建netty server线程池的LinkedBlockingQueue
         */
        this.clientRequestThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getClientRequestThreadPoolQueueCapacity());
        /*
         * 创建netty远程通信执行器线程池，用作默认的请求处理线程池，线程名以ClientRequestExecutorThread_为前缀
         */
        this.clientRequestExecutor = new ThreadPoolExecutor(this.namesrvConfig.getClientRequestThreadPoolNums(), this.namesrvConfig.getClientRequestThreadPoolNums(), 1000 * 60, TimeUnit.MILLISECONDS, this.clientRequestThreadPoolQueue, new ThreadFactoryImpl("ClientRequestExecutorThread_")) {
            @Override
            protected <T> RunnableFuture<T> newTaskFor(final Runnable runnable, final T value) {
                return new FutureTaskExt<>(runnable, value);
            }
        };
    }

    private void initiateSslContext() {
        // Register a listener to reload SslContext
        if (TlsSystemConfig.tlsMode == TlsMode.DISABLED) {
            return;
        }

        String[] watchFiles = {TlsSystemConfig.tlsServerCertPath, TlsSystemConfig.tlsServerKeyPath, TlsSystemConfig.tlsServerTrustCertPath};

        FileWatchService.Listener listener = new FileWatchService.Listener() {
            boolean certChanged, keyChanged = false;

            @Override
            public void onChanged(String path) {
                if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                    LOGGER.info("The trust certificate changed, reload the ssl context");
                    ((NettyRemotingServer) remotingServer).loadSslContext();
                }
                if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                    certChanged = true;
                }
                if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                    keyChanged = true;
                }
                if (certChanged && keyChanged) {
                    LOGGER.info("The certificate and private key changed, reload the ssl context");
                    certChanged = keyChanged = false;
                    ((NettyRemotingServer) remotingServer).loadSslContext();
                }
            }
        };

        try {
            fileWatchService = new FileWatchService(watchFiles, listener);
        } catch (Exception e) {
            LOGGER.warn("FileWatchService created error, can't load the certificate dynamically");
        }
    }

    private void printWaterMark() {
        WATER_MARK_LOG.info("[WATERMARK] ClientQueueSize:{} ClientQueueSlowTime:{} " + "DefaultQueueSize:{} DefaultQueueSlowTime:{}", this.clientRequestThreadPoolQueue.size(), headSlowTimeMills(this.clientRequestThreadPoolQueue), this.defaultThreadPoolQueue.size(), headSlowTimeMills(this.defaultThreadPoolQueue));
    }

    private long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable firstRunnable = q.peek();

        if (firstRunnable instanceof FutureTaskExt) {
            final Runnable inner = ((FutureTaskExt<?>) firstRunnable).getRunnable();
            if (inner instanceof RequestTask) {
                slowTimeMills = System.currentTimeMillis() - ((RequestTask) inner).getCreateTimestamp();
            }
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    private void registerProcessor() {
        if (namesrvConfig.isClusterTest()) {
            // 集群测试
            this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()), this.defaultExecutor);
        } else {
            // Support get route info only temporarily
            // 暂时只支持获得路由信息
            //将remotingExecutor绑定到DefaultRequestProcessor上，用作默认的请求处理线程池
            //将DefaultRequestProcessor绑定到remotingServer的defaultRequestProcessor属性上
            ClientRequestProcessor clientRequestProcessor = new ClientRequestProcessor(this);
            this.remotingServer.registerProcessor(RequestCode.GET_ROUTEINFO_BY_TOPIC, clientRequestProcessor, this.clientRequestExecutor);

            this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.defaultExecutor);
        }
    }

    private void initiateRpcHooks() {
        this.remotingServer.registerRPCHook(new ZoneRouteRPCHook());
    }

    public void start() throws Exception {
        //调用remotingServer的启动方法
        this.remotingServer.start();

        // In test scenarios where it is up to OS to pick up an available port, set the listening port back to config
        if (0 == nettyServerConfig.getListenPort()) {
            nettyServerConfig.setListenPort(this.remotingServer.localListenPort());
        }
        //调用remotingClient的启动方法
        this.remotingClient.updateNameServerAddressList(Collections.singletonList(NetworkUtil.getLocalAddress()
            + ":" + nettyServerConfig.getListenPort()));
        this.remotingClient.start();

        if (this.fileWatchService != null) {
            //监听tts相关文件是否发生变化
            this.fileWatchService.start();
        }

        this.routeInfoManager.start();
    }

    public void shutdown() {
        //关闭nettyClient
        this.remotingClient.shutdown();
        //关闭nettyServer
        this.remotingServer.shutdown();
        //关闭线程池
        this.defaultExecutor.shutdown();
        this.clientRequestExecutor.shutdown();
        //关闭定时任务
        this.scheduledExecutorService.shutdown();
        this.scanExecutorService.shutdown();
        this.routeInfoManager.shutdown();

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
