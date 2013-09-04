package edu.uw.zookeeper.clients.common;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceMonitor;

public class GuiceRuntimeModule extends AbstractModule {

    public static GuiceRuntimeModule create(RuntimeModule runtime) {
        return new GuiceRuntimeModule(runtime);
    }

    private final RuntimeModule runtime;
    
    public GuiceRuntimeModule(RuntimeModule runtime) {
        this.runtime = runtime;
    }
    
    @Override
    protected void configure() {
        bind(Executor.class).to(ExecutorService.class).in(Singleton.class);
        bind(ExecutorService.class).to(ListeningExecutorService.class).in(Singleton.class);
        bind(ScheduledExecutorService.class).to(ListeningScheduledExecutorService.class).in(Singleton.class);
    }
    
    @Provides @Singleton
    public RuntimeModule getRuntimeModule() {
        return runtime;
    }

    @Provides @Singleton
    public ServiceMonitor getServiceMonitor(
            RuntimeModule runtime) {
        return runtime.getServiceMonitor();
    }

    @Provides @Singleton
    public Configuration getConfiguration(
            RuntimeModule runtime) {
        return runtime.getConfiguration();
    }

    @Provides @Singleton
    public Factory<ThreadFactory> getThreadFactory(
            RuntimeModule runtime) {
        return runtime.getThreadFactory();
    }

    @Provides @Singleton
    public ListeningExecutorServiceFactory getExecutors(
            RuntimeModule runtime) {
        return runtime.getExecutors();
    }
    
    @Provides @Singleton
    public ListeningExecutorService getListeningExecutor(
            ListeningExecutorServiceFactory instance) {
        return instance.get(ListeningExecutorService.class);
    }
    
    @Provides @Singleton
    public ListeningScheduledExecutorService getListeningScheduledExecutor(
            ListeningExecutorServiceFactory instance) {
        return instance.get(ListeningScheduledExecutorService.class);
    }
}
