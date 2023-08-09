
CREATE TABLE cloudfront_logs
(
    LOG_DATE				                DATE,
    LOG_TIME				                TIME,
    LOG_TIMESTAMP			                TIMESTAMP,
    EDGE_LOCATION			                VARCHAR(1024),
    SC_BYTES				                BIGINT,
    C_IP                                    VARCHAR(1024),
    CS_METHOD                               VARCHAR(1024),
    CS_HOST                                 VARCHAR(1024),
    CS_URI_STEM                             VARCHAR(1024),
    SC_STATUS                               VARCHAR(1024),
    CS_REFERER                              VARCHAR(10024),
    CS_USER_AGENT                           VARCHAR(1024),
    CS_URI_QUERY                            VARCHAR(10024),
    CS_COOKIE                               VARCHAR(10024),
    EDGE_RESULT_TYPE                        VARCHAR(1024),
    EDGE_REQUEST_ID                         VARCHAR(1024),
    HOST_HEADER                             VARCHAR(1024),
    CS_PROTOCOL                             VARCHAR(1024),
    CS_BYTES                                BIGINT,
    TIME_TAKEN                              DOUBLE,
    X_FORWARDED_FOR                         VARCHAR(101024),
    SSL_PROTOCOL                            VARCHAR(1024),
    SSL_CIPHER                              VARCHAR(1024),
    EDGE_RESPONSE_RESULT_TYPE               VARCHAR(1024),
    CS_PROTOCOL_VERSION                     VARCHAR(1024),
    FLE_STATUS                              VARCHAR(1024),
    FLE_ENCRYPTED_FIELDS                    VARCHAR(1024),
    C_PORT                                  BIGINT,
    TIME_TO_FIRST_BYTE                      DOUBLE,
    EDGE_DETAILED_RESULT_TYPE               VARCHAR(1024),
    CONTENT_TYPE                            VARCHAR(1024),
    CONTENT_LENGTH                          BIGINT,
    RANGE_START                             VARCHAR(1024),
    RANGE_END                               VARCHAR(1024)

)


