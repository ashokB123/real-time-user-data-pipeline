-- =============================================================================
-- Random User Pipeline: Postgres schema
--
-- Follows the same 4-table breakdown as the reference article (users,
-- addresses, logins, contacts), but links them with a shared user_id foreign
-- key so a row in "addresses" can actually be traced back to the person it
-- belongs to. The reference article's version inserts into all four tables
-- independently with no shared key, which makes it impossible to join them
-- back together later -- this fixes that.
--
-- Run this once, manually, before the pipeline starts writing data:
--   psql -U airflow -d airflow -f schema.sql
-- (or point it at a different database/user if you're keeping pipeline data
-- separate from Airflow's own metadata database -- recommended, see note
-- at the bottom of this file)
-- =============================================================================

CREATE TABLE IF NOT EXISTS users (
    id              SERIAL PRIMARY KEY,
    uuid            UUID UNIQUE,              -- from result.login.uuid; stable per-person identifier from the API
    gender          VARCHAR(20),
    title           VARCHAR(20),
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(255) UNIQUE,
    date_of_birth   DATE,
    registered_date DATE,                     -- from result.registered.date
    nationality     VARCHAR(10),               -- from result.nat
    picture_url     TEXT,                      -- result.picture.large
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),  -- when our producer pulled this record
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()   -- when this row was inserted into Postgres
);

CREATE TABLE IF NOT EXISTS addresses (
    id            SERIAL PRIMARY KEY,
    user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    street_number VARCHAR(20),
    street_name   VARCHAR(255),
    city          VARCHAR(100),
    state         VARCHAR(100),
    country       VARCHAR(100),
    postcode      VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS logins (
    id            SERIAL PRIMARY KEY,
    user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    username      VARCHAR(100),
    uuid          UUID,                       -- duplicated from users.uuid for convenience in login-focused queries
    md5           VARCHAR(32),
    sha1          VARCHAR(40),
    sha256        VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS contacts (
    id      SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    phone   VARCHAR(50),
    cell    VARCHAR(50)
);

-- Helpful indexes for the kinds of lookups a campaign-analytics use case
-- would actually run (per the article's "marketing/sales/accounting" scenario)
CREATE INDEX IF NOT EXISTS idx_users_nationality ON users(nationality);
CREATE INDEX IF NOT EXISTS idx_users_registered_date ON users(registered_date);
CREATE INDEX IF NOT EXISTS idx_addresses_country ON addresses(country);
CREATE INDEX IF NOT EXISTS idx_addresses_user_id ON addresses(user_id);
CREATE INDEX IF NOT EXISTS idx_logins_user_id ON logins(user_id);
CREATE INDEX IF NOT EXISTS idx_contacts_user_id ON contacts(user_id);

-- -----------------------------------------------------------------------------
-- NOTE on which database to run this against:
-- Your Postgres container currently only has the "airflow" database (used for
-- Airflow's own metadata -- DAG runs, task history, etc). Running pipeline
-- data through that same database works, but mixing your own tables into
-- Airflow's internal schema is messy long-term. Consider creating a
-- dedicated database first:
--   docker compose exec postgres psql -U airflow -d postgres -c "CREATE DATABASE randomuser OWNER airflow;"
-- then run this file against "randomuser" instead of "airflow".
-- -----------------------------------------------------------------------------
