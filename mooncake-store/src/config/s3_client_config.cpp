#include "s3_client_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

S3ClientConfig S3ClientConfig::FromEnvironment() {
    S3ClientConfig config;
    config.region = Environ::ReadOr(
        S3ClientEnvironmentVariables::MOONCAKE_AWS_REGION, config.region);
    config.s3_endpoint =
        Environ::ReadOr(S3ClientEnvironmentVariables::MOONCAKE_AWS_S3_ENDPOINT,
                        config.s3_endpoint);
    config.bucket_name =
        Environ::ReadOr(S3ClientEnvironmentVariables::MOONCAKE_AWS_BUCKET_NAME,
                        config.bucket_name);
    config.access_key_id = Environ::ReadOr(
        S3ClientEnvironmentVariables::MOONCAKE_AWS_ACCESS_KEY_ID,
        config.access_key_id);
    config.secret_access_key = Environ::ReadOr(
        S3ClientEnvironmentVariables::MOONCAKE_AWS_SECRET_ACCESS_KEY,
        config.secret_access_key);
    config.use_virtual_addressing = Environ::ReadOr(
        S3ClientEnvironmentVariables::MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING,
        config.use_virtual_addressing);
    config.use_https = Environ::ReadOr(
        S3ClientEnvironmentVariables::MOONCAKE_AWS_USE_HTTPS, config.use_https);
    config.request_checksum_calculation = Environ::ReadOr(
        S3ClientEnvironmentVariables::MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION,
        config.request_checksum_calculation);
    config.response_checksum_validation = Environ::ReadOr(
        S3ClientEnvironmentVariables::MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION,
        config.response_checksum_validation);
    config.connect_timeout = std::chrono::milliseconds(Environ::ReadOr(
        S3ClientEnvironmentVariables::MOONCAKE_AWS_CONNECT_TIMEOUT_MS,
        config.connect_timeout.count()));
    config.request_timeout = std::chrono::milliseconds(Environ::ReadOr(
        S3ClientEnvironmentVariables::MOONCAKE_AWS_REQUEST_TIMEOUT_MS,
        config.request_timeout.count()));
    return config;
}

}  // namespace mooncake
