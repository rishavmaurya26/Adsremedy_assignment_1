-- ============================================================
--  init.sql  –  runs automatically when the container starts
-- ============================================================

CREATE TABLE IF NOT EXISTS employees_clean (
    employee_id     INTEGER PRIMARY KEY,
    first_name      VARCHAR(50)     NOT NULL,
    last_name       VARCHAR(50)     NOT NULL,
    full_name       VARCHAR(100)    NOT NULL,
    email           VARCHAR(100)    UNIQUE NOT NULL,
    email_domain    VARCHAR(50),
    hire_date       DATE            NOT NULL,
    job_title       VARCHAR(100),
    department      VARCHAR(50),
    salary          DECIMAL(10,2),
    salary_band     VARCHAR(20),
    manager_id      INTEGER,
    address         TEXT,
    city            VARCHAR(50),
    state           VARCHAR(2),
    zip_code        VARCHAR(10),
    birth_date      DATE,
    age             INTEGER,
    tenure_years    DECIMAL(3,1),
    status          VARCHAR(20)     DEFAULT 'Active',
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_department  ON employees_clean(department);
CREATE INDEX IF NOT EXISTS idx_salary_band ON employees_clean(salary_band);
CREATE INDEX IF NOT EXISTS idx_status      ON employees_clean(status);
