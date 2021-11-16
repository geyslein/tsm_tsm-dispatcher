BEGIN;
--
-- Create model Thing
--
CREATE TABLE "thing" ("id" bigserial NOT NULL PRIMARY KEY, "name" varchar(200) NOT NULL, "uuid" uuid NOT NULL UNIQUE, "description" text NULL, "properties" jsonb NULL);
--
-- Create model Datastream
--
CREATE TABLE "datastream" ("id" bigserial NOT NULL PRIMARY KEY, "name" varchar(200) NOT NULL, "description" text NULL, "properties" jsonb NULL, "position" varchar(200) NOT NULL, "thing_id" bigint NOT NULL);
--
-- Create model Observation
--
CREATE TABLE "observation" ("id" bigserial NOT NULL PRIMARY KEY, "phenomenon_time_start" timestamp with time zone NULL, "phenomenon_time_end" timestamp with time zone NULL, "result_time" timestamp with time zone NOT NULL, "result_type" smallint NOT NULL, "result_number" double precision NULL, "result_string" varchar(200) NULL, "result_json" jsonb NULL, "result_boolean" boolean NULL, "result_latitude" double precision NULL, "result_longitude" double precision NULL, "result_altitude" double precision NULL, "result_quality" jsonb NULL, "valid_time_start" timestamp with time zone NULL, "valid_time_end" timestamp with time zone NULL, "parameters" jsonb NULL, "datastream_id" bigint NOT NULL);
ALTER TABLE "datastream" ADD CONSTRAINT "datastream_thing_id_position_9f2cfe68_uniq" UNIQUE ("thing_id", "position");
ALTER TABLE "datastream" ADD CONSTRAINT "datastream_thing_id_f55522a4_fk_thing_id" FOREIGN KEY ("thing_id") REFERENCES "thing" ("id") DEFERRABLE INITIALLY DEFERRED;
CREATE INDEX "datastream_thing_id_f55522a4" ON "datastream" ("thing_id");
ALTER TABLE "observation" ADD CONSTRAINT "observation_datastream_id_result_time_1d043396_uniq" UNIQUE ("datastream_id", "result_time");
ALTER TABLE "observation" ADD CONSTRAINT "observation_datastream_id_77f5c4fb_fk_datastream_id" FOREIGN KEY ("datastream_id") REFERENCES "datastream" ("id") DEFERRABLE INITIALLY DEFERRED;
CREATE INDEX "observation_datastream_id_77f5c4fb" ON "observation" ("datastream_id");
COMMIT;
