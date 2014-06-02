package edu.uw.zookeeper.common;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons.AutomatonListener;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;

public abstract class ForwardingProtocolCodec<I,O,U,V> implements ProtocolCodec<I,O,U,V> {

    @Override
    public Class<? extends U> encodeType() {
        return delegate().encodeType();
    }

    @Override
    public void encode(I input, ByteBuf output) throws IOException {
        delegate().encode(input, output);
    }

    @Override
    public Class<? extends V> decodeType() {
        return delegate().decodeType();
    }

    @Override
    public Optional<? extends O> decode(ByteBuf input) throws IOException {
        return delegate().decode(input);
    }

    @Override
    public ProtocolState state() {
        return delegate().state();
    }

    @Override
    public Optional<Automaton.Transition<ProtocolState>> apply(ProtocolState input) {
        return delegate().apply(input);
    }

    @Override
    public void subscribe(AutomatonListener<ProtocolState> listener) {
        delegate().subscribe(listener);
    }

    @Override
    public boolean unsubscribe(AutomatonListener<ProtocolState> listener) {
        return delegate().unsubscribe(listener);
    }

    protected abstract ProtocolCodec<I,O,U,V> delegate();
}
