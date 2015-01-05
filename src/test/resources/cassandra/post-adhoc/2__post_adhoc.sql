--liquibase formatted sql
--changeset dbaas:dbaas_2 dbms:cassandra
CREATE TABLE post_adhoc2 (
      schedule_id int,
      time timestamp,
      value double,
      PRIMARY KEY (schedule_id, time)
      );
