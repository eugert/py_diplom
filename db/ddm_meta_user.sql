-- Create the user 
create user DDM_META
  default tablespace USERS
  temporary tablespace TEMP
  password expire;
-- Grant/Revoke role privileges 
grant connect to DDM_META;
grant resource to DDM_META;
-- Grant/Revoke system privileges 
grant alter session to DDM_META;
grant create database link to DDM_META;
grant create procedure to DDM_META;
grant create sequence to DDM_META;
grant create session to DDM_META;
grant create synonym to DDM_META;
grant create table to DDM_META;
grant create view to DDM_META;
grant unlimited tablespace to DDM_META;