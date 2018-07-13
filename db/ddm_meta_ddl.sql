--rem Differences between <CURRENT USER>@PYTHON_LEARN and SYSTEM@PYTHON_LEARN, created on 07.07.2018
--rem Press Apply button, or run in Command Window or SQL*Plus connected as SYSTEM@PYTHON_LEARN

-----------------------------
--  New table meta_tables  --
-----------------------------
-- Create table
create table META_TABLES
(
  id          NUMBER not null,
  schema_name VARCHAR2(100),
  table_name  VARCHAR2(100)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create/Recreate primary, unique and foreign key constraints 
alter table META_TABLES
  add constraint META_TABLES_PK primary key (ID)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );

----------------------------------
--  New table meta_tables_cols  --
----------------------------------
-- Create table
create table META_TABLES_COLS
(
  id           NUMBER not null,
  table_id     NUMBER,
  col_name     VARCHAR2(100),
  col_position NUMBER
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create/Recreate primary, unique and foreign key constraints 
alter table META_TABLES_COLS
  add constraint META_TABLES_COLS_PK primary key (ID)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
alter table META_TABLES_COLS
  add constraint META_TABLES_COLS_FK foreign key (TABLE_ID)
  references META_TABLES (ID) on delete cascade;

----------------------------
--  New table set_stream  --
----------------------------
-- Create table
create table SET_STREAM
(
  id          NUMBER not null,
  stream_code VARCHAR2(100)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create/Recreate primary, unique and foreign key constraints 
alter table SET_STREAM
  add constraint SET_STREAM primary key (ID)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );

----------------------------------
--  New table set_stream_table  --
----------------------------------
-- Create table
create table SET_STREAM_TABLE
(
  id        NUMBER not null,
  stream_id NUMBER,
  table_id  NUMBER
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create/Recreate primary, unique and foreign key constraints 
alter table SET_STREAM_TABLE
  add constraint SET_STREAM_TABLE_PK primary key (ID)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
alter table SET_STREAM_TABLE
  add constraint SET_STREAM_TABLE_FK_STREAM foreign key (STREAM_ID)
  references SET_STREAM (ID);
alter table SET_STREAM_TABLE
  add constraint SET_STREAM_TABLE_FK_TABLE foreign key (TABLE_ID)
  references META_TABLES (ID);

