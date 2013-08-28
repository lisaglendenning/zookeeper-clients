package edu.uw.zookeeper.clients.trace;

import java.io.File;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.clients.common.RuntimeModuleProvider;
import edu.uw.zookeeper.common.Configuration;

public abstract class TraceClientModule extends ClientApplicationModule {

    public static class Module extends AbstractModule {

        public static Module create() {
            return new Module();
        }
        
        @Override
        protected void configure() {
            install(JacksonModule.create());
        }
        
        @Provides @Singleton
        public File getTraceFile(Configuration configuration) {
            return new File(Trace.getTraceFileConfiguration(configuration));
        }
    }
    
    protected final Injector injector;

    protected TraceClientModule(RuntimeModule runtime) {
        super(runtime);
        this.injector = createInjector(runtime);
    }
    
    public Injector getInjector() {
        return injector;
    }
    
    protected Injector createInjector(RuntimeModule runtime) {
        return Guice.createInjector(
                RuntimeModuleProvider.create(runtime), 
                module());
    }
    
    protected com.google.inject.Module module() {
        return Module.create();
    }
}
