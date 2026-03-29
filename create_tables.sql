-- ============================================================
-- Property Management App — Create Table Statements
-- Target: Google BigQuery
-- Replace `your_dataset` with your actual dataset name
-- ============================================================

CREATE TABLE IF NOT EXISTS `your_dataset.properties` (
  property_id   INT64   NOT NULL,
  name          STRING  NOT NULL,
  address       STRING  NOT NULL,
  city          STRING  NOT NULL,
  state         STRING  NOT NULL,
  postal_code   STRING  NOT NULL,
  property_type STRING  NOT NULL,
  tenant_name   STRING,
  monthly_rent  FLOAT64 NOT NULL
);

CREATE TABLE IF NOT EXISTS `your_dataset.income` (
  income_id   INT64   NOT NULL,
  property_id INT64   NOT NULL,
  amount      FLOAT64 NOT NULL,
  date        DATE    NOT NULL,
  description STRING
);

CREATE TABLE IF NOT EXISTS `your_dataset.expenses` (
  expense_id  INT64   NOT NULL,
  property_id INT64   NOT NULL,
  amount      FLOAT64 NOT NULL,
  date        DATE    NOT NULL,
  category    STRING  NOT NULL,
  vendor      STRING,
  description STRING
);