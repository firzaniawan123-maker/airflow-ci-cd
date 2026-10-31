CREATE DATABASE IF NOT EXISTS fintech_payment_oltp;

USE fintech_payment_oltp;

CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_external_id VARCHAR(100),
    full_name VARCHAR(150),
    email VARCHAR(150),
    phone_number VARCHAR(30),
    gender VARCHAR(20),
    date_of_birth DATE,
    address TEXT,
    city VARCHAR(100),
    province VARCHAR(100),
    postal_code VARCHAR(10),
    country VARCHAR(100),
    customer_segment VARCHAR(30),
    registration_channel VARCHAR(30),
    account_status VARCHAR(30),
    kyc_status VARCHAR(30),
    registered_at DATETIME,
    last_login_at DATETIME,
    created_at DATETIME,
    updated_at DATETIME,

    INDEX idx_customer_external_id (customer_external_id),
    INDEX idx_customer_updated_at (updated_at)
);

CREATE TABLE merchants (
    merchant_id VARCHAR(50) PRIMARY KEY,
    merchant_external_id VARCHAR(100),
    merchant_name VARCHAR(150),
    legal_name VARCHAR(150),
    business_type VARCHAR(50),
    business_category VARCHAR(100),
    industry_code VARCHAR(30),
    merchant_tier VARCHAR(30),
    city VARCHAR(100),
    province VARCHAR(100),
    country VARCHAR(100),
    postal_code VARCHAR(10),
    address TEXT,
    settlement_cycle VARCHAR(10),
    settlement_account VARCHAR(100),
    bank_name VARCHAR(100),
    merchant_status VARCHAR(30),
    onboarding_channel VARCHAR(30),
    registered_at DATETIME,
    activated_at DATETIME,
    created_at DATETIME,
    updated_at DATETIME,

    INDEX idx_merchant_external_id (merchant_external_id),
    INDEX idx_merchant_updated_at (updated_at)
);

CREATE TABLE payment_methods (
    payment_method_id VARCHAR(50) PRIMARY KEY,
    method_code VARCHAR(50),
    method_name VARCHAR(100),
    method_type VARCHAR(50),
    provider_name VARCHAR(100),
    provider_code VARCHAR(50),
    issuer_name VARCHAR(100),
    network_name VARCHAR(100),
    channel VARCHAR(30),
    currency VARCHAR(10),
    is_international BOOLEAN,
    status VARCHAR(30),
    effective_from DATETIME,
    effective_until DATETIME,
    created_at DATETIME,
    updated_at DATETIME,

    INDEX idx_payment_method_code (method_code),
    INDEX idx_payment_method_updated_at (updated_at)
);

CREATE TABLE payment_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    transaction_reference VARCHAR(100),
    customer_id VARCHAR(50),
    merchant_id VARCHAR(50),
    payment_method_id VARCHAR(50),
    parent_transaction_id VARCHAR(50),
    transaction_type VARCHAR(30),
    transaction_status VARCHAR(30),
    payment_status VARCHAR(30),
    currency VARCHAR(10),
    gross_amount DECIMAL(18,2),
    discount_amount DECIMAL(18,2),
    tax_amount DECIMAL(18,2),
    service_fee DECIMAL(18,2),
    platform_fee DECIMAL(18,2),
    customer_fee DECIMAL(18,2),
    merchant_fee DECIMAL(18,2),
    net_amount DECIMAL(18,2),
    exchange_rate DECIMAL(18,6),
    original_amount DECIMAL(18,2),
    description TEXT,
    order_id VARCHAR(100),
    invoice_number VARCHAR(100),
    device_id VARCHAR(100),
    ip_address VARCHAR(45),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    country_code VARCHAR(10),
    channel VARCHAR(30),
    source_system VARCHAR(100),
    initiated_at DATETIME,
    authorized_at DATETIME,
    completed_at DATETIME,
    failed_at DATETIME,
    failure_reason VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME,

    CONSTRAINT fk_payment_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id),

    CONSTRAINT fk_payment_merchant
        FOREIGN KEY (merchant_id)
        REFERENCES merchants(merchant_id),

    CONSTRAINT fk_payment_method
        FOREIGN KEY (payment_method_id)
        REFERENCES payment_methods(payment_method_id),

    INDEX idx_payment_customer (customer_id),
    INDEX idx_payment_merchant (merchant_id),
    INDEX idx_payment_method (payment_method_id),
    INDEX idx_payment_reference (transaction_reference),
    INDEX idx_payment_updated_at (updated_at),
    INDEX idx_payment_created_at (created_at)
);

CREATE TABLE payment_transaction_events (
    event_id VARCHAR(50) PRIMARY KEY,
    transaction_id VARCHAR(50),
    event_type VARCHAR(50),
    event_status VARCHAR(30),
    event_sequence INT,
    previous_status VARCHAR(30),
    new_status VARCHAR(30),
    event_timestamp DATETIME,
    processing_time_ms INT,
    error_code VARCHAR(50),
    error_message TEXT,
    source_system VARCHAR(100),
    service_name VARCHAR(100),
    retry_count INT,
    created_at DATETIME,

    CONSTRAINT fk_event_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES payment_transactions(transaction_id),

    INDEX idx_event_transaction (transaction_id),
    INDEX idx_event_timestamp (event_timestamp)
);

CREATE TABLE refunds (
    refund_id VARCHAR(50) PRIMARY KEY,
    transaction_id VARCHAR(50),
    merchant_id VARCHAR(50),
    customer_id VARCHAR(50),
    refund_reference VARCHAR(100),
    refund_type VARCHAR(30),
    refund_reason VARCHAR(255),
    refund_status VARCHAR(30),
    requested_amount DECIMAL(18,2),
    approved_amount DECIMAL(18,2),
    refund_fee DECIMAL(18,2),
    net_refund_amount DECIMAL(18,2),
    requested_at DATETIME,
    approved_at DATETIME,
    completed_at DATETIME,
    failure_reason VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME,

    CONSTRAINT fk_refund_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES payment_transactions(transaction_id),

    CONSTRAINT fk_refund_merchant
        FOREIGN KEY (merchant_id)
        REFERENCES merchants(merchant_id),

    CONSTRAINT fk_refund_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id),

    INDEX idx_refund_transaction (transaction_id),
    INDEX idx_refund_updated_at (updated_at)
);

CREATE TABLE chargebacks (
    chargeback_id VARCHAR(50) PRIMARY KEY,
    transaction_id VARCHAR(50),
    customer_id VARCHAR(50),
    merchant_id VARCHAR(50),
    chargeback_reference VARCHAR(100),
    chargeback_reason_code VARCHAR(50),
    chargeback_reason VARCHAR(255),
    chargeback_status VARCHAR(30),
    dispute_amount DECIMAL(18,2),
    fee_amount DECIMAL(18,2),
    currency VARCHAR(10),
    dispute_opened_at DATETIME,
    response_due_at DATETIME,
    resolved_at DATETIME,
    resolution VARCHAR(100),
    created_at DATETIME,
    updated_at DATETIME,

    CONSTRAINT fk_chargeback_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES payment_transactions(transaction_id),

    CONSTRAINT fk_chargeback_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id),

    CONSTRAINT fk_chargeback_merchant
        FOREIGN KEY (merchant_id)
        REFERENCES merchants(merchant_id),

    INDEX idx_chargeback_transaction (transaction_id),
    INDEX idx_chargeback_updated_at (updated_at)
);

CREATE TABLE settlement_providers (
    provider_id VARCHAR(50) PRIMARY KEY,
    provider_code VARCHAR(50),
    provider_name VARCHAR(100),
    provider_type VARCHAR(50),
    country VARCHAR(100),
    currency VARCHAR(10),
    settlement_method VARCHAR(50),
    settlement_cycle VARCHAR(10),
    contact_email VARCHAR(150),
    status VARCHAR(30),
    effective_from DATE,
    effective_until DATE,
    created_at DATETIME,
    updated_at DATETIME,

    INDEX idx_provider_code (provider_code),
    INDEX idx_provider_updated_at (updated_at)
);

CREATE TABLE settlement_batches (
    settlement_batch_id VARCHAR(50) PRIMARY KEY,
    batch_reference VARCHAR(100),
    merchant_id VARCHAR(50),
    provider_id VARCHAR(50),
    settlement_date DATE,
    settlement_cycle VARCHAR(10),
    currency VARCHAR(10),
    transaction_count INT,
    gross_amount DECIMAL(18,2),
    refund_amount DECIMAL(18,2),
    chargeback_amount DECIMAL(18,2),
    fee_amount DECIMAL(18,2),
    tax_amount DECIMAL(18,2),
    adjustment_amount DECIMAL(18,2),
    net_settlement_amount DECIMAL(18,2),
    settlement_status VARCHAR(30),
    bank_account VARCHAR(100),
    bank_code VARCHAR(50),
    payout_reference VARCHAR(100),
    processed_at DATETIME,
    completed_at DATETIME,
    failure_reason VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME,

    CONSTRAINT fk_batch_merchant
        FOREIGN KEY (merchant_id)
        REFERENCES merchants(merchant_id),

    CONSTRAINT fk_batch_provider
        FOREIGN KEY (provider_id)
        REFERENCES settlement_providers(provider_id),

    INDEX idx_batch_merchant (merchant_id),
    INDEX idx_batch_provider (provider_id),
    INDEX idx_batch_date (settlement_date),
    INDEX idx_batch_updated_at (updated_at)
);

CREATE TABLE settlement_transactions (
    settlement_transaction_id VARCHAR(50) PRIMARY KEY,
    settlement_batch_id VARCHAR(50),
    transaction_id VARCHAR(50),
    transaction_reference VARCHAR(100),
    merchant_id VARCHAR(50),
    provider_transaction_id VARCHAR(100),
    settlement_reference VARCHAR(100),
    settlement_status VARCHAR(30),
    currency VARCHAR(10),
    gross_amount DECIMAL(18,2),
    refund_amount DECIMAL(18,2),
    chargeback_amount DECIMAL(18,2),
    fee_amount DECIMAL(18,2),
    tax_amount DECIMAL(18,2),
    adjustment_amount DECIMAL(18,2),
    net_amount DECIMAL(18,2),
    settlement_amount DECIMAL(18,2),
    settlement_date DATE,
    processed_at DATETIME,
    completed_at DATETIME,
    failure_reason VARCHAR(255),
    source_file VARCHAR(255),
    source_system VARCHAR(100),
    created_at DATETIME,
    updated_at DATETIME,

    CONSTRAINT fk_settlement_batch
        FOREIGN KEY (settlement_batch_id)
        REFERENCES settlement_batches(settlement_batch_id),

    CONSTRAINT fk_settlement_merchant
        FOREIGN KEY (merchant_id)
        REFERENCES merchants(merchant_id),

    INDEX idx_settlement_batch (settlement_batch_id),
    INDEX idx_settlement_transaction (transaction_id),
    INDEX idx_settlement_merchant (merchant_id),
    INDEX idx_settlement_date (settlement_date),
    INDEX idx_settlement_updated_at (updated_at)
);

CREATE TABLE settlement_adjustments (
    adjustment_id VARCHAR(50) PRIMARY KEY,
    settlement_batch_id VARCHAR(50),
    transaction_id VARCHAR(50),
    merchant_id VARCHAR(50),
    adjustment_type VARCHAR(50),
    adjustment_reason VARCHAR(255),
    adjustment_amount DECIMAL(18,2),
    currency VARCHAR(10),
    reference_number VARCHAR(100),
    adjustment_status VARCHAR(30),
    created_at DATETIME,
    updated_at DATETIME,

    CONSTRAINT fk_adjustment_batch
        FOREIGN KEY (settlement_batch_id)
        REFERENCES settlement_batches(settlement_batch_id),

    CONSTRAINT fk_adjustment_merchant
        FOREIGN KEY (merchant_id)
        REFERENCES merchants(merchant_id),

    INDEX idx_adjustment_batch (settlement_batch_id),
    INDEX idx_adjustment_transaction (transaction_id),
    INDEX idx_adjustment_updated_at (updated_at)
);

