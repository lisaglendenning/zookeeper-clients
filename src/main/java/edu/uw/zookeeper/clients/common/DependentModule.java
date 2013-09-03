package edu.uw.zookeeper.clients.common;

import java.util.List;

import com.google.inject.AbstractModule;
import com.google.inject.Module;

public abstract class DependentModule extends AbstractModule {

    @Override
    protected void configure() {
        installDependentModules();
    }
    
    protected void installDependentModules() {
        for (Module m: getDependentModules()) {
            install(m);
        }
    }

    protected abstract List<Module> getDependentModules();
}
