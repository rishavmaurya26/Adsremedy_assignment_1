-- ============================================================
--  verify_and_analyse.sql
--  Run after the Spark pipeline to confirm data was loaded
--  correctly and explore the cleaned data.
--
--  Usage:
--    docker exec -it employee_postgres \
--      psql -U sparkuser -d employeedb -f /path/to/verify_and_analyse.sql
-- ============================================================


-- ── 1. Row count ─────────────────────────────────────────────────────────────
SELECT COUNT(*) AS total_employees FROM employees_clean;


-- ── 2. Sample rows ───────────────────────────────────────────────────────────
SELECT employee_id, full_name, email, department, salary, salary_band, tenure_years, age
FROM   employees_clean
LIMIT  10;


-- ── 3. Salary band distribution ──────────────────────────────────────────────
SELECT
    salary_band,
    COUNT(*)                        AS headcount,
    ROUND(AVG(salary), 2)           AS avg_salary,
    MIN(salary)                     AS min_salary,
    MAX(salary)                     AS max_salary
FROM employees_clean
GROUP BY salary_band
ORDER BY avg_salary;


-- ── 4. Department headcount ───────────────────────────────────────────────────
SELECT
    department,
    COUNT(*)                        AS headcount,
    ROUND(AVG(salary), 2)           AS avg_salary
FROM employees_clean
GROUP BY department
ORDER BY headcount DESC;


-- ── 5. Status distribution ───────────────────────────────────────────────────
SELECT status, COUNT(*) AS cnt
FROM   employees_clean
GROUP  BY status
ORDER  BY cnt DESC;


-- ── 6. Age & tenure statistics ───────────────────────────────────────────────
SELECT
    ROUND(AVG(age),          1) AS avg_age,
    MIN(age)                    AS min_age,
    MAX(age)                    AS max_age,
    ROUND(AVG(tenure_years), 1) AS avg_tenure_years,
    MIN(tenure_years)           AS min_tenure,
    MAX(tenure_years)           AS max_tenure
FROM employees_clean;


-- ── 7. Top 5 email domains ───────────────────────────────────────────────────
SELECT
    email_domain,
    COUNT(*) AS cnt
FROM   employees_clean
GROUP  BY email_domain
ORDER  BY cnt DESC
LIMIT  5;


-- ── 8. Data quality checks on loaded data ────────────────────────────────────
SELECT
    COUNT(*)                                        AS total,
    COUNT(*) FILTER (WHERE salary IS NULL)          AS null_salary,
    COUNT(*) FILTER (WHERE age IS NULL)             AS null_age,
    COUNT(*) FILTER (WHERE birth_date IS NULL)      AS null_birth_date,
    COUNT(*) FILTER (WHERE manager_id IS NULL)      AS null_manager,
    COUNT(*) FILTER (WHERE salary_band = 'Unknown') AS unknown_band
FROM employees_clean;
