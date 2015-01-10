--liquibase formatted sql
--changeset dbaas:dbaas_1 dbms:cassandra
CREATE TABLE post_adhoc1 (
      schedule_id int,
      time timestamp,
      value double,
      PRIMARY KEY (schedule_id, time)
      );
