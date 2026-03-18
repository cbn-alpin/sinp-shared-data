BEGIN;

\echo '-------------------------------------------------------------------------------'
\echo 'Copy CSV data into gn_imports schema and validation table.'
\echo 'Rights: superuser'
\echo 'GeoNature database compatibility : v2.4.1'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Remove imports validation table if already exists'
DROP TABLE IF EXISTS gn_imports.:validationImportTable ;


\echo '-------------------------------------------------------------------------------'
\echo 'Create imports validation table from "t_validations" with additional fields'
CREATE TABLE gn_imports.:validationImportTable AS
    SELECT
        NULL::INT AS gid,
        uuid_attached_row AS unique_id_sinp,
        id_nomenclature_valid_status,
        NULL::varchar AS validator,
        validation_comment AS comment,
        validation_auto AS "automatic",
        validation_date AS creation_date,
        additional_data,
        meta_create_date,
        meta_update_date,
        last_action AS meta_last_action
    FROM gn_commons.t_validations
WITH NO DATA ;


\echo '-------------------------------------------------------------------------------'
\echo 'Add primary key on imports validation table'
\set importTablePk 'pk_':validationImportTable
ALTER TABLE gn_imports.:validationImportTable
	ALTER COLUMN gid ADD GENERATED ALWAYS AS IDENTITY,
	ADD CONSTRAINT :importTablePk PRIMARY KEY(gid);


\echo '-------------------------------------------------------------------------------'
\echo 'Attribute imports validation to GeoNature DB owner'
ALTER TABLE gn_imports.:validationImportTable OWNER TO :gnDbOwner ;


\echo '-------------------------------------------------------------------------------'
\echo 'Copy CVS file to import validation table'
COPY gn_imports.:validationImportTable (
    unique_id_sinp,
    id_nomenclature_valid_status,
    validator,
    comment,
    "automatic",
    creation_date,
    additional_data,
    meta_create_date,
    meta_update_date,
    meta_last_action
)
FROM :'csvFilePath'
WITH CSV HEADER DELIMITER E'\t' NULL '\N' ;


\echo '-------------------------------------------------------------------------------'
\echo 'Create indexes on imports validation table'
\set uuidIdx 'idx_unique_':validationImportTable'_uuid'
CREATE UNIQUE INDEX :uuidIdx
    ON gn_imports.:validationImportTable USING btree (unique_id_sinp);

\set updateDateIdx 'idx_':validationImportTable'_meta_update_date'
CREATE INDEX :updateDateIdx
    ON gn_imports.:validationImportTable USING btree (meta_update_date);

\set lastActionIdx 'idx_':validationImportTable'_meta_last_action'
CREATE INDEX :lastActionIdx
    ON gn_imports.:validationImportTable USING btree (meta_last_action);


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
