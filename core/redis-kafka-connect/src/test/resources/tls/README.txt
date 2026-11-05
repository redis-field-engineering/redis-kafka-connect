TEST CREDENTIALS ONLY. These keys and certificates are public fixtures, never production credentials.

The CA signs server (localhost/127.0.0.1), wrong-host (redis.invalid), and client certificates.
Certificates are valid from November 2026 to October 2126. The client-encrypted.key is the same
client key encrypted as PKCS#8 with password test-key-password (PBE-SHA1-3DES).
The unencrypted keys are PKCS#8 PEM. The CA private key is deliberately not distributed.
RedisTlsIntegrationTests uses these fixtures to exercise trust, hostname verification,
password authentication, mutual TLS, and encrypted client-key configuration.
