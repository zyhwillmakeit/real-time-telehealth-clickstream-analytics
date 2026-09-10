BEGIN;

CREATE SCHEMA IF NOT EXISTS source;

CREATE TABLE IF NOT EXISTS source.customer_master (
    customer_id VARCHAR(12) PRIMARY KEY,
    signup_date DATE NOT NULL,
    region VARCHAR(16) NOT NULL,
    membership_plan VARCHAR(16) NOT NULL,
    acquisition_channel VARCHAR(24) NOT NULL,
    account_status VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,

    CONSTRAINT ck_customer_id_format CHECK (customer_id ~ '^cus_[0-9]{8}$'),
    CONSTRAINT ck_customer_region CHECK (region IN ('midwest', 'northeast', 'south', 'west')),
    CONSTRAINT ck_customer_membership_plan CHECK (membership_plan IN ('basic', 'premium')),
    CONSTRAINT ck_customer_acquisition_channel
        CHECK (acquisition_channel IN ('employer', 'organic', 'paid_search', 'referral')),
    CONSTRAINT ck_customer_account_status CHECK (account_status IN ('active', 'closed', 'suspended')),
    CONSTRAINT ck_customer_update_order CHECK (updated_at >= created_at)
);

CREATE INDEX IF NOT EXISTS ix_customer_master_updated_at
    ON source.customer_master (updated_at);

COMMENT ON TABLE source.customer_master IS
    'Synthetic operational customer attributes used for product analytics; contains no clinical data.';

COMMENT ON COLUMN source.customer_master.updated_at IS
    'Source-system change timestamp used for snapshot audit and freshness measurement.';

COMMENT ON COLUMN source.customer_master.is_deleted IS
    'Logical deletion marker retained in snapshots so historical facts remain interpretable.';

COMMIT;
