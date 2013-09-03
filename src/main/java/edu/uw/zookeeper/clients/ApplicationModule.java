package edu.uw.zookeeper.clients;

import java.util.List;

import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.internal.Lists;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.clients.common.DependentModule;
import edu.uw.zookeeper.clients.common.GuiceRuntimeModule;
import edu.uw.zookeeper.clients.common.InjectorServiceLocator;
import edu.uw.zookeeper.clients.common.ServiceLocator;
import edu.uw.zookeeper.common.RuntimeModule;

public class ApplicationModule<T extends ZooKeeperApplication.RuntimeBuilder<?>> extends DependentModule {

    protected final T builder;
    
    public ApplicationModule(T builder) {
        this.builder = builder;
    }
    
    @Override
    protected void configure() {
        super.configure();
        bind(ServiceLocator.class).to(InjectorServiceLocator.class).in(Singleton.class);
    }
    
    @Provides @Singleton
    public RuntimeModule getRuntimeModule() {
        return builder.getRuntimeModule();
    }

    @Override
    protected List<Module> getDependentModules() {
        return Lists.<Module>newArrayList(GuiceRuntimeModule.create());
    }
}
