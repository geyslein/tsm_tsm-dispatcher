BEGIN;

--- CREATE VIEW "THINGS" ---
CREATE OR REPLACE VIEW "THINGS" AS SELECT
    id as "ID",
    description as "DESCRIPTION",
    properties as "PROPERTIES",
    name as "NAME"
FROM thing;

--- CREATE VIEW SENSORS ---
CREATE OR REPLACE VIEW "SENSORS" AS SELECT
    id as "ID",
    description as "DESCRIPTION",
    text '' as "ENCODING_TYPE",
    text '' as "METADATA",
    short_name as "NAME",
    jsonb_build_object(
        'serial_number', serial_number,
        'model', model
        ) as "PROPERTIES"
FROM public.sms_device;
--- properties (json) or metadata (text) probably need to be built from several fields
--- (e.g. model and serial number,...)

--- CREATE VIEW OBS_PROPERTIES ---
CREATE OR REPLACE VIEW "OBS_PROPERTIES" AS SELECT DISTINCT
    reverse(split_part(reverse(property_uri), '/', 2))::bigint as "ID",
    property_name as "NAME",
    property_uri "DEFINITION",
    text '' as "DESCRIPTION",
    jsonb_build_object('property_1', 'some property',
                       'property_2', 'some other property')
        as "PROPERTIES"
FROM public.sms_device_property sms_dp;

--- CREATE VIEW OBSERVATION ---
CREATE OR REPLACE VIEW "date_stream" AS
    with datastream_map as (
        select datastream_id ,begin_date ,end_date, device_property_id from public.sms_datastream dl
    )
    select distinct  tsm_obs.result_time,coalesce(m.datastream_id) as ds_id, m.device_property_id
    from observation tsm_obs
    join datastream_map m on (tsm_obs.result_time >= m.begin_date) and case
        when m.end_date is null then tsm_obs.result_time < '9999-12-31'::timestamp
        else (tsm_obs.result_time < m.end_date)
    end;

CREATE OR REPLACE VIEW "dp_ds_mapping" AS
select
	tsm_obs.result_time,
	tsm_obs.datastream_id as tsm_datastream_id,
    ds.device_property_id
from observation tsm_obs
join date_stream ds on tsm_obs.result_time = ds.result_time and tsm_obs.datastream_id = ds.ds_id::bigint;

CREATE OR REPLACE VIEW "OBSERVATIONS" AS SELECT
    phenomenon_time_start as "PHENOMENON_TIME_START",
    phenomenon_time_end as "PHENOMENON_TIME_END",
    tsm_obs.result_time as "RESULT_TIME",
    CASE
        WHEN result_type = 1 THEN 0
        WHEN result_type = 2 THEN 1
        WHEN result_type = 3 THEN 2
        ELSE result_type
    END as "RESULT_TYPE",
    result_number as "RESULT_NUMBER",
    result_boolean as "RESULT_BOOLEAN",
    result_json as "RESULT_JSON",
    result_string as "RESULT_STRING",
    result_quality as "RESULT_QUALITY",
    valid_time_start as "VALID_TIME_START",
    valid_time_end as "VALID_TIME_END",
    parameters as "PARAMETERS",
    ddm.device_property_id::bigint as "DATASTREAM_ID",
    bigint '1' as "FEATURE_ID",
    null as "MULTI_DATASTREAM_ID",
    row_number() over() as "ID"
FROM observation tsm_obs
INNER JOIN dp_ds_mapping ddm ON tsm_obs.result_time = ddm.result_time AND tsm_obs.datastream_id = ddm.tsm_datastream_id;

--- CREATE VIEW DATASTREAMS ---
DROP VIEW IF EXISTS "DATASTREAMS" CASCADE;
CREATE OR REPLACE VIEW "DATASTREAMS" AS SELECT DISTINCT
    sms_dp.id::bigint as "ID",
    sms_dp.label as "NAME",
    tsm_ds.description as "DESCRIPTION",
    text '' as "OBSERVATION_TYPE",
    obs.phenomenon_time_start as "PHENOMENON_TIME_START",
    obs.phenomenon_time_end as "PHENOMENON_TIME_END",
    obs.result_time_start as "RESULT_TIME_START",
    obs.result_time_end as "RESULT_TIME_END",
    sms_dp.device_id as "SENSOR_ID",
    reverse(split_part(reverse(sms_dp.property_uri), '/', 2))::bigint as "OBS_PROPERTY_ID",
    tsm_ds.thing_id::bigint as "THING_ID",
    sms_dp.property_name as "UNIT_NAME",
    sms_dp.unit_name as "UNIT_SYMBOL",
    sms_dp.label as "UNIT_DEFINITION",
    null as "OBSERVED_AREA",
    '{}' as "PROPERTIES",
    bigint '0' as "LAST_FOI_ID"
FROM datastream tsm_ds
INNER JOIN public.sms_datastream sms_ds ON tsm_ds.id::bigint = sms_ds.datastream_id::bigint
INNER JOIN public.sms_device_property sms_dp ON sms_ds.device_property_id::bigint = sms_dp.id::bigint
INNER JOIN (
    SELECT
        "DATASTREAM_ID",
        MIN("RESULT_TIME") as result_time_start,
        MAX("RESULT_TIME") as result_time_end,
		MIN("PHENOMENON_TIME_START") as phenomenon_time_start,
        MAX("PHENOMENON_TIME_END") as phenomenon_time_end
	FROM "OBSERVATIONS"
  	GROUP BY "DATASTREAM_ID"
    )
    obs ON sms_dp.id = obs."DATASTREAM_ID";

COMMIT;
