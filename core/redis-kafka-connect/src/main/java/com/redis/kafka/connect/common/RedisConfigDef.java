package com.redis.kafka.connect.common;

import org.apache.kafka.common.config.ConfigDef;

import com.redis.spring.batch.common.AbstractOperationExecutor;

import io.lettuce.core.RedisURI;

public abstract class RedisConfigDef extends ConfigDef {

    public static final String CLUSTER_CONFIG = "redis.cluster";

    private static final boolean CLUSTER_DEFAULT = false;

    private static final String CLUSTER_DOC = "Connect to a Redis Cluster database";

    public static final String HOST_CONFIG = "redis.host";

    private static final String HOST_DEFAULT = "localhost";

    private static final String HOST_DOC = "The Redis host to connect to";

    public static final String PORT_CONFIG = "redis.port";

    private static final int PORT_DEFAULT = RedisURI.DEFAULT_REDIS_PORT;

    private static final String PORT_DOC = "The Redis port to connect to";

    public static final String URI_CONFIG = "redis.uri";

    private static final String URI_DEFAULT = "";

    private static final String URI_DOC = "URI of the Redis database to connect to, e.g. redis://redis-12000.redis.com:12000. For secure connections use rediss URI scheme, e.g. rediss://...";

    public static final String USERNAME_CONFIG = "redis.username";

    private static final String USERNAME_DEFAULT = "";

    private static final String USERNAME_DOC = "Username to use to connect to Redis";

    public static final String PASSWORD_CONFIG = "redis.password";

    private static final String PASSWORD_DEFAULT = "";

    private static final String PASSWORD_DOC = "Password to use to connect to Redis";

    public static final String TIMEOUT_CONFIG = "redis.timeout";

    private static final long TIMEOUT_DEFAULT = RedisURI.DEFAULT_TIMEOUT;

    private static final String TIMEOUT_DOC = "Redis command timeout in seconds";

    public static final String POOL_MAX_CONFIG = "redis.pool";

    private static final int POOL_MAX_DEFAULT = AbstractOperationExecutor.DEFAULT_POOL_SIZE;

    private static final String POOL_MAX_DOC = "Max pool connections";

    public static final String TLS_CONFIG = "redis.tls";

    private static final boolean TLS_DEFAULT = false;

    private static final String TLS_DOC = "Establish a secure TLS connection";

    public static final String INSECURE_CONFIG = "redis.insecure";

    private static final boolean INSECURE_DEFAULT = false;

    private static final String INSECURE_DOC = "Allow insecure connections (e.g. invalid certificates) to Redis when using SSL";

    public static final String KEY_CONFIG = "redis.key.file";

    public static final String KEY_DEFAULT = "";

    private static final String KEY_DOC = "PKCS#8 private key file to authenticate with (PEM format)";

    public static final String KEY_CERT_CONFIG = "redis.key.cert";

    public static final String KEY_CERT_DEFAULT = "";

    private static final String KEY_CERT_DOC = "X.509 certificate chain file to authenticate with (PEM format)";

    public static final String KEY_PASSWORD_CONFIG = "redis.key.password";

    private static final String KEY_PASSWORD_DEFAULT = "";

    private static final String KEY_PASSWORD_DOC = "Password of the private key file. Leave empty if key file is not password-protected";

    public static final String CACERT_CONFIG = "redis.cacert";

    public static final String CACERT_DEFAULT = "";

    private static final String CACERT_DOC = "X.509 CA certificate file to verify with";

    protected RedisConfigDef() {
        defineConfigs();
    }

    protected RedisConfigDef(ConfigDef base) {
        super(base);
        defineConfigs();
    }

    private void defineConfigs() {
        define(CLUSTER_CONFIG, Type.BOOLEAN, CLUSTER_DEFAULT, Importance.MEDIUM, CLUSTER_DOC);
        define(HOST_CONFIG, Type.STRING, HOST_DEFAULT, Importance.HIGH, HOST_DOC);
        define(PORT_CONFIG, Type.INT, PORT_DEFAULT, Importance.HIGH, PORT_DOC);
        define(URI_CONFIG, Type.STRING, URI_DEFAULT, Importance.MEDIUM, URI_DOC);
        define(TLS_CONFIG, Type.BOOLEAN, TLS_DEFAULT, Importance.MEDIUM, TLS_DOC);
        define(INSECURE_CONFIG, Type.BOOLEAN, INSECURE_DEFAULT, Importance.MEDIUM, INSECURE_DOC);
        define(PASSWORD_CONFIG, Type.PASSWORD, PASSWORD_DEFAULT, Importance.MEDIUM, PASSWORD_DOC);
        define(USERNAME_CONFIG, Type.STRING, USERNAME_DEFAULT, Importance.MEDIUM, USERNAME_DOC);
        define(TIMEOUT_CONFIG, Type.LONG, TIMEOUT_DEFAULT, Importance.MEDIUM, TIMEOUT_DOC);
        define(POOL_MAX_CONFIG, Type.INT, POOL_MAX_DEFAULT, Importance.MEDIUM, POOL_MAX_DOC);
        define(KEY_CONFIG, Type.STRING, KEY_DEFAULT, Importance.MEDIUM, KEY_DOC);
        define(KEY_CERT_CONFIG, Type.STRING, KEY_CERT_DEFAULT, Importance.MEDIUM, KEY_CERT_DOC);
        define(KEY_PASSWORD_CONFIG, Type.PASSWORD, KEY_PASSWORD_DEFAULT, Importance.MEDIUM, KEY_PASSWORD_DOC);
        define(CACERT_CONFIG, Type.STRING, CACERT_DEFAULT, Importance.MEDIUM, CACERT_DOC);
    }

}
