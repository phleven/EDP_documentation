/*
-- Revoke privileges before dropping the user
REVOKE ALL PRIVILEGES ON DATABASE irs_group_lakebase FROM irs_lakebase_user;
REVOKE ALL PRIVILEGES ON SCHEMA public FROM irs_lakebase_user;
 
REVOKE ALL PRIVILEGES ON DATABASE irs_group_lakebase FROM lakebase_user_role;
REVOKE ALL PRIVILEGES ON SCHEMA public FROM lakebase_user_role;
REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM lakebase_user_role;
 
DROP USER irs_lakebase_user;
DROP ROLE lakebase_user_role;
*/
 
CREATE USER irs_lakebase_user WITH PASSWORD 'irslakebase123';
 
CREATE ROLE lakebase_user_role;
 
GRANT ALL PRIVILEGES ON DATABASE irs_group_lakebase TO lakebase_user_role;
GRANT ALL PRIVILEGES ON SCHEMA public TO lakebase_user_role;
 
-- Grant read access to all existing tables in a schema
GRANT SELECT ON ALL TABLES IN SCHEMA public TO lakebase_user_role;
 
GRANT lakebase_user_role TO irs_lakebase_user;

