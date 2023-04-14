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

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;

public class NamesrvStartup {

    private final static Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private final static Logger logConsole = LoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_LOGGER_NAME);
    private static Properties properties = null;
    private static NamesrvConfig namesrvConfig = null;
    private static NettyServerConfig nettyServerConfig = null;
    private static NettyClientConfig nettyClientConfig = null;
    private static ControllerConfig controllerConfig = null;

    // name srv启动入口
    public static void main(String[] args) {
        main0(args);
        controllerManagerMain();
    }

    public static NamesrvController main0(String[] args) {
        try {
            /**
             * 解析命令行和配置文件
             */
            parseCommandlineAndConfigFile(args);
            /*
             * 1、创建NamesrvController并启动name srv
             */
            NamesrvController controller = createAndStartNamesrvController();
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static ControllerManager controllerManagerMain() {
        try {
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                return createAndStartControllerManager();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    public static void parseCommandlineAndConfigFile(String[] args) throws Exception {
        //设置RocketMQ的版本信息，属性名为rocketmq.remoting.version
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //PackageConflictDetect.detectFastjson();
        /*jar包启动时，构建命令行操作的指令，使用main方法启动可以忽略*/
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        //mqnamesrv命令文件
        CommandLine commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
            return;
        }
        //创建NameServer的配置类，包含NameServer的配置，比如ROCKETMQ_HOME
        namesrvConfig = new NamesrvConfig();
        //和NettyServer的配置类
        nettyServerConfig = new NettyServerConfig();
        //和NettyClient的配置类
        nettyClientConfig = new NettyClientConfig();
        //netty服务的监听端口设置为9876
        nettyServerConfig.setListenPort(9876);
        //判断命令行中是否包含字符'c'，即是否包含通过命令行指定配置文件的命令
        //例如，启动Broker的时候添加的 -c /Volumes/Samsung/Idea/rocketmq/config/conf/broker.conf命令
        if (commandLine.hasOption('c')) {
            /*解析配置文件并且存入NamesrvConfig和NettyServerConfig中，没有的话就不用管*/
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
                properties = new Properties();
                properties.load(in);
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);
                MixAll.properties2Object(properties, nettyClientConfig);
                if (namesrvConfig.isEnableControllerInNamesrv()) {
                    controllerConfig = new ControllerConfig();
                    MixAll.properties2Object(properties, controllerConfig);
                }
                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }
        //把命令行的配置解析到namesrvConfig
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);
        /*判断命令行中是否包含字符'p'，如果存在则打印配置信息并结束jvm运行，没有的话就不用管*/
        if (commandLine.hasOption('p')) {
            MixAll.printObjectProperties(logConsole, namesrvConfig);
            MixAll.printObjectProperties(logConsole, nettyServerConfig);
            MixAll.printObjectProperties(logConsole, nettyClientConfig);
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                MixAll.printObjectProperties(logConsole, controllerConfig);
            }
            System.exit(0);
        }
        //如果不存在ROCKETMQ_HOME的配置，那么打印异常并退出程序，这就是最开始启动NameServer是抛出异常的位置
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }
        /*一系列日志的配置*/
        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);

    }

    public static NamesrvController createAndStartNamesrvController() throws Exception {
        // 创建NamesrvController
        NamesrvController controller = createNamesrvController();
        // 启动NamesrvController
        start(controller);
        NettyServerConfig serverConfig = controller.getNettyServerConfig();
        /*
         * 启动成功打印日志
         */
        String tip = String.format("The Name Server boot success. serializeType=%s, address %s:%d", RemotingCommand.getSerializeTypeConfigInThisServer(), serverConfig.getBindAddress(), serverConfig.getListenPort());
        log.info(tip);
        System.out.printf("%s%n", tip);
        return controller;
    }

    /**
     * 创建NamesrvController 并设置配置信息
     * @return
     */
    public static NamesrvController createNamesrvController() {
        /*
         * 根据namesrvConfig和nettyServerConfig创建NamesrvController
         */
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig, nettyClientConfig);
        // remember all configs to prevent discard
        // 将所有的-c的外部配置信息保存到NamesrvController中的Configuration对象属性的allConfigs属性中
        controller.getConfiguration().registerConfig(properties);
        return controller;
    }

    public static NamesrvController start(final NamesrvController controller) throws Exception {
        //不能为null
        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }
        /*
         * 1 初始化NettyServer
         * 创建netty远程服务，初始化Netty线程池，注册请求处理器，配置定时任务，用于扫描并移除不活跃的Broker等操作。
         * initialize的大概步骤为：
         * 1.加载KV配置并存储到kvConfigManager内部的configTable属性中。
         * 2.创建NameServer的netty远程服务。remotingServer是一个基于Netty的用于NameServer与Broker、Consumer、Producer进行网络通信的服务端。
         * 3.创建netty远程通信执行器线程池remotingExecutor，线程数默认8，线程名以RemotingExecutorThread_为前缀，用作默认的请求处理线程池。
         * 4.注册默认请求处理器DefaultRequestProcessor到remotingServer中。
         * 5.启动两个定时任务。其中一个每隔十秒钟检测不活跃的Broker并清理相关路由信息，这是一个核心知识点，另一个任务则是每隔十分钟打印kv配置信息。
         */
        boolean initResult = controller.initialize();
        //初始化失败则退出程序
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }
        /*
         * 2 添加关闭钩子方法，在NameServer关闭之前执行，进行一些内存清理、对象销毁等操作
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));
        /*
         * 3 启动NettyServer，并进行监听
         */
        controller.start();

        return controller;
    }

    public static ControllerManager createAndStartControllerManager() throws Exception {
        ControllerManager controllerManager = createControllerManager();
        start(controllerManager);
        String tip = "The ControllerManager boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
        log.info(tip);
        System.out.printf("%s%n", tip);
        return controllerManager;
    }

    public static ControllerManager createControllerManager() throws Exception {
        NettyServerConfig controllerNettyServerConfig = (NettyServerConfig) nettyServerConfig.clone();
        ControllerManager controllerManager = new ControllerManager(controllerConfig, controllerNettyServerConfig, nettyClientConfig);
        // remember all configs to prevent discard
        controllerManager.getConfiguration().registerConfig(properties);
        return controllerManager;
    }

    public static ControllerManager start(final ControllerManager controllerManager) throws Exception {

        if (null == controllerManager) {
            throw new IllegalArgumentException("ControllerManager is null");
        }

        boolean initResult = controllerManager.initialize();
        if (!initResult) {
            controllerManager.shutdown();
            System.exit(-3);
        }

        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controllerManager.shutdown();
            return null;
        }));

        controllerManager.start();

        return controllerManager;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static void shutdown(final ControllerManager controllerManager) {
        controllerManager.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config items");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
