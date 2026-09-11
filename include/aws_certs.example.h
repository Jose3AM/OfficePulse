#pragma once

// Copiar este archivo como aws_certs.h y sustituir los placeholders.
// aws_certs.h esta excluido de Git y no debe publicarse.
const char AWS_ROOT_CA[] = R"EOF(
-----BEGIN CERTIFICATE-----
YOUR_AWS_ROOT_CA
-----END CERTIFICATE-----
)EOF";

const char AWS_DEVICE_CERT[] = R"EOF(
-----BEGIN CERTIFICATE-----
YOUR_DEVICE_CERTIFICATE
-----END CERTIFICATE-----
)EOF";

const char AWS_PRIVATE_KEY[] = R"EOF(
-----BEGIN PRIVATE KEY-----
YOUR_DEVICE_PRIVATE_KEY
-----END PRIVATE KEY-----
)EOF";
