/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.MapUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Plugin;
import org.lealone.db.PluginBase;
import org.lealone.db.PluginManager;

public abstract class SchedulerFactoryBase extends PluginBase implements SchedulerFactory {

    protected Scheduler[] schedulers = new Scheduler[0];

    protected final AtomicInteger bindIndex = new AtomicInteger();
    protected Thread[] bindThreads = new Thread[0];

    protected SchedulerFactoryBase(Map<String, String> config, Scheduler[] schedulers) {
        super("SchedulerFactory");
        boolean embedded = false;
        if (schedulers == null) {
            // 如果未指定调度器，那么使用嵌入式调度器
            schedulers = createSchedulers(EmbeddedScheduler.class.getName(), config);
            embedded = true;
        }
        init(config);
        for (Scheduler scheduler : schedulers) {
            scheduler.setSchedulerFactory(this);
        }
        this.schedulers = schedulers;
        this.bindThreads = new Thread[schedulers.length];
        if (embedded) // 嵌入式场景自动启动调度器
            start();
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return SchedulerFactory.class;
    }

    @Override
    public Scheduler getScheduler(int id) {
        return schedulers[id];
    }

    @Override
    public Scheduler[] getSchedulers() {
        return schedulers;
    }

    @Override
    public int getSchedulerCount() {
        return schedulers.length;
    }

    @Override
    public Scheduler bindScheduler(Thread thread) {
        int index = bindIndex.getAndIncrement();
        if (index >= schedulers.length) {
            synchronized (this) {
                for (int i = 0; i < schedulers.length; i++) {
                    if (!bindThreads[i].isAlive()) {
                        bindThreads[i] = thread;
                        return schedulers[i];
                    }
                }
            }
            // 如果不返回null的话，可以尝试动态增加新的调度线程的方案
            return null;
        }
        bindThreads[index] = thread;
        return schedulers[index];
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        Scheduler[] schedulers = this.schedulers;
        for (Scheduler scheduler : schedulers) {
            scheduler.start();
        }
        super.start();
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        Scheduler[] schedulers = this.schedulers;
        for (Scheduler scheduler : schedulers) {
            scheduler.stop();
        }
        for (Scheduler scheduler : schedulers) {
            Thread thread = scheduler.getThread();
            if (Thread.currentThread() != thread) {
                try {
                    if (thread != null)
                        thread.join();
                } catch (InterruptedException e) {
                }
            }
        }
        this.schedulers = new Scheduler[0];
        this.bindThreads = new Thread[0];
        bindIndex.set(0);
        super.stop();
    }

    public static SchedulerFactory create(Map<String, String> config) {
        return create(config, null);
    }

    public static synchronized SchedulerFactory create(Map<String, String> config,
            Scheduler[] schedulers) {
        SchedulerFactory factory = null;
        String key = "scheduler_factory_type";
        String type = MapUtils.getString(config, key, null);
        if (type == null || type.equalsIgnoreCase("RoundRobin"))
            factory = new RoundRobinFactory(config, schedulers);
        else if (type.equalsIgnoreCase("Random"))
            factory = new RandomFactory(config, schedulers);
        else if (type.equalsIgnoreCase("LoadBalance"))
            factory = new LoadBalanceFactory(config, schedulers);
        else {
            throw new RuntimeException("Unknow " + key + ": " + type);
        }
        return factory;
    }

    private static class RandomFactory extends SchedulerFactoryBase {

        private static final Random random = new Random();

        protected RandomFactory(Map<String, String> config, Scheduler[] schedulers) {
            super(config, schedulers);
        }

        @Override
        public Scheduler getScheduler() {
            int index = random.nextInt(schedulers.length);
            return schedulers[index];
        }
    }

    private static class RoundRobinFactory extends SchedulerFactoryBase {

        private static final AtomicInteger index = new AtomicInteger(0);

        protected RoundRobinFactory(Map<String, String> config, Scheduler[] schedulers) {
            super(config, schedulers);
        }

        @Override
        public Scheduler getScheduler() {
            return schedulers[getAndIncrementIndex(index) % schedulers.length];
        }
    }

    private static class LoadBalanceFactory extends SchedulerFactoryBase {

        protected LoadBalanceFactory(Map<String, String> config, Scheduler[] schedulers) {
            super(config, schedulers);
        }

        @Override
        public Scheduler getScheduler() {
            long minLoad = Long.MAX_VALUE;
            int index = 0;
            for (int i = 0, size = schedulers.length; i < size; i++) {
                long load = schedulers[i].getLoad();
                if (load < minLoad) {
                    index = i;
                    minLoad = load;
                }
            }
            return schedulers[index];
        }
    }

    // 变成负数时从0开始
    public static int getAndIncrementIndex(AtomicInteger index) {
        int i = index.getAndIncrement();
        if (i < 0) {
            if (index.compareAndSet(i, 1)) {
                i = 0;
            } else {
                i = index.getAndIncrement();
            }
        }
        return i;
    }

    private static SchedulerFactory defaultSchedulerFactory;

    public static void setDefaultSchedulerFactory(SchedulerFactory defaultSchedulerFactory) {
        SchedulerFactoryBase.defaultSchedulerFactory = defaultSchedulerFactory;
    }

    public static SchedulerFactory getDefaultSchedulerFactory() {
        return defaultSchedulerFactory;
    }

    public static SchedulerFactory getDefaultSchedulerFactory(String schedulerClassName,
            Properties prop) {
        if (SchedulerFactoryBase.getDefaultSchedulerFactory() == null) {
            Map<String, String> config;
            if (prop != null)
                config = new CaseInsensitiveMap<>(prop);
            else
                config = new CaseInsensitiveMap<>();
            initDefaultSchedulerFactory(schedulerClassName, config);
        }
        return SchedulerFactoryBase.getDefaultSchedulerFactory();
    }

    public static SchedulerFactory getDefaultSchedulerFactory(String schedulerClassName,
            Map<String, String> config) {
        if (SchedulerFactoryBase.getDefaultSchedulerFactory() == null)
            initDefaultSchedulerFactory(schedulerClassName, config);
        return SchedulerFactoryBase.getDefaultSchedulerFactory();
    }

    public static synchronized SchedulerFactory initDefaultSchedulerFactory(String schedulerClassName,
            Map<String, String> config) {
        SchedulerFactory schedulerFactory = SchedulerFactoryBase.getDefaultSchedulerFactory();
        if (schedulerFactory == null) {
            String sf = MapUtils.getString(config, "scheduler_factory", null);
            if (sf != null) {
                schedulerFactory = PluginManager.getPlugin(SchedulerFactory.class, sf);
            } else {
                Scheduler[] schedulers = createSchedulers(schedulerClassName, config);
                schedulerFactory = SchedulerFactory.create(config, schedulers);
            }
            if (!schedulerFactory.isInited())
                schedulerFactory.init(config);
            SchedulerFactoryBase.setDefaultSchedulerFactory(schedulerFactory);
        }
        return schedulerFactory;
    }

    public static Scheduler[] createSchedulers(String schedulerClassName, Map<String, String> config) {
        int schedulerCount = MapUtils.getSchedulerCount(config);
        Scheduler[] schedulers = new Scheduler[schedulerCount];
        for (int i = 0; i < schedulerCount; i++) {
            schedulers[i] = Utils.newInstance(schedulerClassName, i, schedulerCount, config);
        }
        return schedulers;
    }

    public static Scheduler getScheduler(String schedulerClassName, ConnectionInfo ci) {
        Scheduler scheduler = ci.getScheduler();
        if (scheduler == null) {
            SchedulerFactory sf = getDefaultSchedulerFactory(schedulerClassName, ci.getProperties());
            scheduler = getScheduler(sf, ci);
        }
        return scheduler;
    }

    public static Scheduler getScheduler(String schedulerClassName, ConnectionInfo ci,
            Map<String, String> config) {
        Scheduler scheduler = ci.getScheduler();
        if (scheduler == null) {
            SchedulerFactory sf = getDefaultSchedulerFactory(schedulerClassName, config);
            scheduler = getScheduler(sf, ci);
        }
        return scheduler;
    }

    private static Scheduler getScheduler(SchedulerFactory sf, ConnectionInfo ci) {
        Scheduler scheduler = sf.getScheduler();
        ci.setScheduler(scheduler);
        if (!sf.isStarted())
            sf.start();
        return scheduler;
    }
}
