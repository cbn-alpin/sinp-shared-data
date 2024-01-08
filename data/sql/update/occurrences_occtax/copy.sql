BEGIN;

\echo '-------------------------------------------------------------------------------'
\echo 'Copy CSV data into gn_imports schema and occurrences occtax table.'
\echo 'Rights: superuser'
\echo 'GeoNature database compatibility : v2.4.1'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Remove imports occurrences occtax table if already exists'
DROP TABLE IF EXISTS gn_imports.:ooImportTable ;


\echo '-------------------------------------------------------------------------------'
\echo 'Create imports occurrences occtax table from "t_occurrences_occtax" with additional fields'
CREATE TABLE gn_imports.:ooImportTable AS
    SELECT
        NULL::INT AS gid,
        unique_id_occurence_occtax AS unique_id_sinp_occtax,
        id_nomenclature_obs_technique,
        id_nomenclature_bio_condition,
        id_nomenclature_bio_status,
        id_nomenclature_naturalness,
        id_nomenclature_exist_proof,
        id_nomenclature_diffusion_level,
        id_nomenclature_observation_status,
        id_nomenclature_blurring,
        id_nomenclature_source_status,
        id_nomenclature_behaviour,
        determiner,
        id_nomenclature_determination_method,
        cd_nom,
        nom_cite,
        meta_v_taref,
        sample_number_proof,
        digital_proof,
        non_digital_proof,
        comment AS comment_description,
        additional_fields,
        NULL::TIMESTAMP AS meta_create_date,
        NULL::TIMESTAMP AS meta_update_date,
        NULL::BPCHAR(1) AS meta_last_action
    FROM pr_occtax.t_occurrences_occtax
WITH NO DATA ;


\echo '-------------------------------------------------------------------------------'
\echo 'Add primary key on imports occurrences occtax table'
\set importTablePk 'pk_':ooImportTable
ALTER TABLE gn_imports.:ooImportTable
	ALTER COLUMN gid ADD GENERATED ALWAYS AS IDENTITY,
	ADD CONSTRAINT :importTablePk PRIMARY KEY(gid);


\echo '-------------------------------------------------------------------------------'
\echo 'Create indexes on imports occurrences occtax table'
\set uniqueIdIdx 'idx_unique_':ooImportTable'_unique_id_occurence_occtax'
CREATE UNIQUE INDEX :uniqueIdIdx
    ON gn_imports.:ooImportTable USING btree (unique_id_occurence_occtax);

\set updateDateIdx 'idx_':ooImportTable'_meta_update_date'
CREATE INDEX :updateDateIdx
    ON gn_imports.:ooImportTable USING btree (meta_update_date);

\set lastActionIdx 'idx_':ooImportTable'_meta_last_action'
CREATE INDEX :lastActionIdx
    ON gn_imports.:ooImportTable USING btree (meta_last_action);


\echo '-------------------------------------------------------------------------------'
\echo 'Attribute imports occurrences occtax to GeoNature DB owner'
ALTER TABLE gn_imports.:ooImportTable OWNER TO :gnDbOwner ;


\echo '-------------------------------------------------------------------------------'
\echo 'Copy CVS file to import occurrences occtax table'
COPY gn_imports.:ooImportTable (
    unique_id_sinp_occtax,
    code_nomenclature_obs_technique,
    code_nomenclature_bio_condition,
    code_nomenclature_bio_status,
    code_nomenclature_naturalness,
    code_nomenclature_exist_proof,
    code_nomenclature_diffusion_level,
    code_nomenclature_observation_status,
    code_nomenclature_blurring,
    code_nomenclature_source_status,
    code_nomenclature_behaviour,
    determiner,
    code_nomenclature_determination_method,
    cd_nom,
    nom_cite,
    meta_v_taxref,
    sample_number_proof,
    digital_proof,
    non_digital_proof,
    comment_description,
    additional_fields,
    meta_create_date,
    meta_update_date,
    meta_last_action
)
FROM :'csvFilePath'
WITH CSV HEADER DELIMITER E'\t' NULL '\N' ;


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
