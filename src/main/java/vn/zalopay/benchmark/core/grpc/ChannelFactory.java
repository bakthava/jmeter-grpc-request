package vn.zalopay.benchmark.core.grpc;

import com.google.common.net.HostAndPort;

import io.grpc.*;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.zalopay.benchmark.core.config.GrpcRequestConfig;

import java.io.File;
import java.util.Map;

import javax.net.ssl.SSLException;

/** Knows how to construct grpc channels. */
public class ChannelFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelFactory.class);

    public static ChannelFactory create() {
        return new ChannelFactory();
    }

    private ChannelFactory() {}

    public ManagedChannel createChannel(
            HostAndPort endpoint,
            boolean tls,
            boolean disableTlsVerification,
            Map<String, String> metadataHash,
            int maxInboundMessageSize,
            int maxInboundMetadataSize) {
        return createChannel(endpoint, tls, disableTlsVerification, metadataHash,
                maxInboundMessageSize, maxInboundMetadataSize, null, null, null);
    }

    public ManagedChannel createChannel(
            HostAndPort endpoint,
            boolean tls,
            boolean disableTlsVerification,
            Map<String, String> metadataHash,
            int maxInboundMessageSize,
            int maxInboundMetadataSize,
            String clientCertFile,
            String clientKeyFile,
            String caCertFile) {
        NettyChannelBuilder managedChannelBuilder =
                createChannelBuilder(endpoint, tls, disableTlsVerification, metadataHash,
                        clientCertFile, clientKeyFile, caCertFile);
        managedChannelBuilder.maxInboundMessageSize(maxInboundMessageSize);
        managedChannelBuilder.maxInboundMetadataSize(maxInboundMetadataSize);
        return managedChannelBuilder.build();
    }

    private NettyChannelBuilder createChannelBuilder(
            HostAndPort endpoint,
            boolean tls,
            boolean disableTlsVerification,
            Map<String, String> metadataHash,
            String clientCertFile,
            String clientKeyFile,
            String caCertFile) {
        if (!tls) {
            return NettyChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort())
                    .negotiationType(NegotiationType.PLAINTEXT)
                    .intercept(metadataInterceptor(metadataHash));
        }
        return createSSLMessageChannel(endpoint, disableTlsVerification, metadataHash,
                clientCertFile, clientKeyFile, caCertFile);
    }

    private NettyChannelBuilder createSSLMessageChannel(
            HostAndPort endpoint,
            boolean disableTlsVerification,
            Map<String, String> metadataHash,
            String clientCertFile,
            String clientKeyFile,
            String caCertFile) {
        return NettyChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort())
                .negotiationType(NegotiationType.TLS)
                .sslContext(createSslContext(disableTlsVerification, clientCertFile, clientKeyFile, caCertFile))
                .intercept(metadataInterceptor(metadataHash));
    }

    private SslContext createSslContext(boolean disableTlsVerification,
            String clientCertFile, String clientKeyFile, String caCertFile) {
        try {
            io.netty.handler.ssl.SslContextBuilder grpcSslContexts = GrpcSslContexts.forClient();
            if (disableTlsVerification) {
                grpcSslContexts.trustManager(InsecureTrustManagerFactory.INSTANCE);
            } else if (caCertFile != null && !caCertFile.isEmpty()) {
                grpcSslContexts.trustManager(new File(caCertFile));
            }
            if (clientCertFile != null && !clientCertFile.isEmpty()
                    && clientKeyFile != null && !clientKeyFile.isEmpty()) {
                grpcSslContexts.keyManager(new File(clientCertFile), new File(clientKeyFile));
            }
            return createSSlContext(grpcSslContexts);
        } catch (SSLException e) {
            LOGGER.error("Error in create SslContext {}", e.getMessage());
            throw new RuntimeException("Error in create SSL connection!", e);
        }
    }

    private SslContext createSSlContext(io.netty.handler.ssl.SslContextBuilder grpcSslContexts)
            throws SSLException {
        try {
            LOGGER.debug("Create SslContext with NPN_AND_ALPN");
            return grpcSslContexts
                    .applicationProtocolConfig(
                            new ApplicationProtocolConfig(
                                    ApplicationProtocolConfig.Protocol.NPN_AND_ALPN,
                                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                                    ApplicationProtocolConfig.SelectedListenerFailureBehavior
                                            .ACCEPT,
                                    ApplicationProtocolNames.HTTP_2))
                    .build();
        } catch (UnsupportedOperationException e) {
            LOGGER.warn("Error in create SslContext with NPN_AND_ALPN {}", e.getMessage());
            return grpcSslContexts
                    .applicationProtocolConfig(
                            new ApplicationProtocolConfig(
                                    ApplicationProtocolConfig.Protocol.ALPN,
                                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                                    ApplicationProtocolConfig.SelectedListenerFailureBehavior
                                            .ACCEPT,
                                    ApplicationProtocolNames.HTTP_2))
                    .build();
        }
    }

    private ClientInterceptor metadataInterceptor(Map<String, String> metadataHash) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    final io.grpc.MethodDescriptor<ReqT, RespT> method,
                    CallOptions callOptions,
                    final Channel next) {
                return new ClientInterceptors.CheckedForwardingClientCall<ReqT, RespT>(
                        next.newCall(method, callOptions)) {
                    @Override
                    protected void checkedStart(
                            Listener<RespT> responseListener, Metadata headers) {
                        for (Map.Entry<String, String> entry : metadataHash.entrySet()) {
                            Metadata.Key<String> key =
                                    Metadata.Key.of(
                                            entry.getKey(), Metadata.ASCII_STRING_MARSHALLER);
                            headers.put(key, entry.getValue());
                        }
                        delegate().start(responseListener, headers);
                    }
                };
            }
        };
    }
}
