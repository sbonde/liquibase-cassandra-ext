--liquibase formatted sql

-- general
-- do make schema changes backward compatible. all changes that change backward compatibility should be done in post-scripts.
-- do not modify a changeset that has already been released, add a new one.
-- do not include data changes in this script.
-- changeset definition format: {author}:{release}_{env} dbms:mysql context:{env}
-- {author}: the author of the changeset
-- {release}: the release containing this changeset. Do not use quotes, '-' or '.'. Safe characters include '0-9, a-z, A-Z, _'
-- {env}: if appropriate, an environment name can be included making the changeset conditional to a specific environmemt. (dev, qa, preprod, prod)
--changeset gs:globalstore_release_1_0_0 dbms:cassandra

-- all tables in this file must be created in keyspace GLOBAL_STORE_ORDERS
-- use global_store_orders;

-- order request from GS OMS
-- request_payload contains entire order with line items, in JSON format
CREATE TABLE gs_order (
        src_order_id text,
        src_org_id text,
        partner_id text,
        partner_order_id text,
        fulfill_order_id text,
        fulfill_org_id text,
        fulfill_dtm timestamp,
        order_type text,
        order_date timestamp,
        order_state text,
        buyer_id text,
        buyer_name text,
        consignee_name text,
        dest_country text,
        affiliate_id text,
        affiliate_date timestamp,
        reflector_id text,
        reflector_date timestamp,
        purchase_site_id text,
        curr_base text,
        curr_local text,
        fx_rate text,
        fx_date timestamp,
        request_payload text,
        request_dtm timestamp,
        snapshot_payload text,
        snapshot_dtm timestamp,
        created_dtm timestamp,
        modified_dtm timestamp,
        attr_ map<text,text>,
        PRIMARY KEY ((src_order_id,src_org_id))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='';