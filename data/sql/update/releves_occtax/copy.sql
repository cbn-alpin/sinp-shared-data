BEGIN;

\echo '-------------------------------------------------------------------------------'
\echo 'Copy CSV data into gn_imports schema and releves occtax table.'
\echo 'Rights: superuser'
\echo 'GeoNature database compatibility : v2.4.1'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Remove imports releves occtax table if already exists'
DROP TABLE IF EXISTS gn_imports.:roImportTable ;


\echo '-------------------------------------------------------------------------------'
\echo 'Create imports releves occtax table from "t_releves_occtax" with additional fields'
CREATE TABLE gn_imports.:roImportTable AS
    SELECT
        NULL::INT AS gid,
        unique_id_sinp_grp,
        id_dataset,
        id_digitiser,
        observers_txt AS observers,
        id_nomenclature_tech_collect_camp AS code_nomenclature_obs_technique,
        id_nomemclature_grp_typ,
        grp_method,
        date_min,
        date_max,
        hour_min,
        hour_max,
        cd_hab,
        altitude_min,
        altitude_max,
        depth_min,
        depth_max,
        place_name,
        meta_device_entry,
        comment AS comment_context,
        geom_local AS geom,
        geom_4326 AS geom,
        id_nomenclature_geo_object_nature,
        precision,
        additional_fields,
        NULL::TIMESTAMP AS meta_create_date,
        NULL::TIMESTAMP AS meta_update_date,
        NULL::BPCHAR(1) AS meta_last_action
    FROM pr_occtax.t_releves_occtax
WITH NO DATA ;


\echo '-------------------------------------------------------------------------------'
\echo 'Add primary key on imports releves occtax table'
\set importTablePk 'pk_':roImportTable
ALTER TABLE gn_imports.:roImportTable
	ALTER COLUMN gid ADD GENERATED ALWAYS AS IDENTITY,
	ADD CONSTRAINT :importTablePk PRIMARY KEY(gid);


\echo '-------------------------------------------------------------------------------'
\echo 'Create indexes on imports releves occtax table'
\set uniqueIdIdx 'idx_unique_':roImportTable'_unique_id_sinp_grp'
CREATE UNIQUE INDEX :uniqueIdIdx
    ON gn_imports.:roImportTable USING btree (unique_id);

\set updateDateIdx 'idx_':roImportTable'_meta_update_date'
CREATE INDEX :updateDateIdx
    ON gn_imports.:roImportTable USING btree (meta_update_date);

\set lastActionIdx 'idx_':roImportTable'_meta_last_action'
CREATE INDEX :lastActionIdx
    ON gn_imports.:roImportTable USING btree (meta_last_action);


\echo '-------------------------------------------------------------------------------'
\echo 'Attribute imports releves occtax to GeoNature DB owner'
ALTER TABLE gn_imports.:roImportTable OWNER TO :gnDbOwner ;


\echo '-------------------------------------------------------------------------------'
\echo 'Copy CVS file to import releves occtax table'
COPY gn_imports.:roImportTable (
    unique_id_sinp_grp,
    code_dataset,
    code_digitiser,
    observers,
    code_nomenclature_obs_technique,
    code_nomenclature_grp_typ,
    grp_method,
    date_min,
    date_max,
    hour_min,
    hour_max,
    cd_hab,
    altitude_min,
    altitude_max,
    depth_min,
    depth_max,
    place_name,
    meta_device_entry,
    comment_context,
    geom,
    code_nomenclature_geo_object_nature,
    precision,
    additional_fields,
    meta_last_action
)
FROM :'csvFilePath'
WITH CSV HEADER DELIMITER E'\t' NULL '\N' ;


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
