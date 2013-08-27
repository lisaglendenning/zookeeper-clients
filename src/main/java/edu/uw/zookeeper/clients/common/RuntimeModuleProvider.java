package edu.uw.zookeeper.clients.common;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.DefaultRuntimeModule;
import edu.uw.zookeeper.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceMonitor;

public class RuntimeModuleProvider extends AbstractModule implements Provider<RuntimeModule> {

    public static RuntimeModuleProvider create() {
        return create(DefaultRuntimeModule.newInstance());
    }
    
    public static RuntimeModuleProvider create(RuntimeModule runtime) {
        return new RuntimeModuleProvider(runtime);
    }
    
    protected final RuntimeModule runtime;
    
    public RuntimeModuleProvider(RuntimeModule runtime) {
        this.runtime = runtime;
    }
    
    @Override
    protected void configure() {
        bind(ServiceLocator.class).to(InjectorServiceLocator.class).in(Singleton.class);
        bind(Executor.class).to(ExecutorService.class).in(Singleton.class);
        bind(ExecutorService.class).to(ListeningExecutorService.class).in(Singleton.class);
        bind(ScheduledExecutorService.class).to(ListeningScheduledExecutorService.class).in(Singleton.class);
    }

    @Provides @Singleton
    public RuntimeModule get() {
        return runtime;
    }

    @Provides @Singleton
    public ServiceMonitor getServiceMonitor(
            RuntimeModule runtime) {
        return runtime.serviceMonitor();
    }

    @Provides @Singleton
    public Configuration getConfiguration(
            RuntimeModule runtime) {
        return runtime.configuration();
    }

    @Provides @Singleton
    public Factory<ThreadFactory> getThreadFactory(
            RuntimeModule runtime) {
        return runtime.threadFactory();
    }

    @Provides @Singleton
    public Factory<? extends Publisher> getPublisherFactory(
            RuntimeModule runtime) {
        return runtime.publisherFactory();
    }

    @Provides @Singleton
    public ListeningExecutorServiceFactory getExecutors(
            RuntimeModule runtime) {
        return runtime.executors();
    }
    
    @Provides @Singleton
    public ListeningExecutorService getListeningExecutor(
            ListeningExecutorServiceFactory instance) {
        return instance.asListeningExecutorServiceFactory().get();
    }
    
    @Provides @Singleton
    public ListeningScheduledExecutorService getListeningScheduledExecutor(
            ListeningExecutorServiceFactory instance) {
        return instance.asListeningScheduledExecutorServiceFactory().get();
    }
}
