BEGIN;

\echo '-------------------------------------------------------------------------------'
\echo 'Copy CSV data into gn_imports schema and occtax table.'
\echo 'Rights: superuser'
\echo 'GeoNature database compatibility : v2.4.1'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Remove imports occtax table if already exists'
DROP TABLE IF EXISTS gn_imports.:occtaxImportTable ;


\echo '-------------------------------------------------------------------------------'
\echo 'Create imports occtax table from "t_releves_occtax, t_occurrences_occtax, cor_counting_occtax" with additional fields'
CREATE TABLE gn_imports.:occtaxImportTable AS
    SELECT
        NULL::INT AS gid,
        unique_id_sinp_grp,
        id_dataset AS code_dataset,
        id_digitiser AS code_digitiser,
        observers_txt AS observers,
        id_nomenclature_tech_collect_campanule AS code_nomenclature_tech_collect_campanule,
        id_nomenclature_grp_typ AS code_nomenclature_grp_typ,
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
        tro."comment" AS comment_context,
        geom_local AS geom,
        id_nomenclature_geo_object_nature AS code_nomenclature_geo_object_nature,
        "precision",
        unique_id_occurence_occtax,
        id_nomenclature_obs_technique AS code_nomenclature_obs_technique,
        id_nomenclature_bio_condition AS code_nomenclature_bio_condition,
        id_nomenclature_bio_status AS code_nomenclature_bio_status,
        id_nomenclature_naturalness AS code_nomenclature_naturalness,
        id_nomenclature_exist_proof AS code_nomenclature_exist_proof,
        id_nomenclature_diffusion_level AS code_nomenclature_diffusion_level,
        id_nomenclature_observation_status AS code_nomenclature_observation_status,
        id_nomenclature_blurring AS code_nomenclature_blurring,
        id_nomenclature_source_status AS code_nomenclature_source_status,
        id_nomenclature_behaviour AS code_nomenclature_behaviour,
        determiner,
        id_nomenclature_determination_method AS code_nomenclature_determination_method,
        cd_nom,
        nom_cite,
        meta_v_taxref,
        sample_number_proof,
        digital_proof,
        non_digital_proof,
        too."comment" AS comment_description,
        unique_id_sinp_occtax,
        id_nomenclature_life_stage AS code_nomenclature_life_stage,
        id_nomenclature_sex AS code_nomenclature_sex,
        id_nomenclature_obj_count AS code_nomenclature_obj_count,
        id_nomenclature_type_count AS code_nomenclature_type_count,
        count_min,
        count_max,
        tro.additional_fields,
        NULL::TIMESTAMP AS meta_create_date,
        NULL::TIMESTAMP AS meta_update_date,
        NULL::BPCHAR(1) AS meta_last_action
FROM pr_occtax.t_releves_occtax tro
	LEFT JOIN pr_occtax.t_occurrences_occtax too ON too.id_releve_occtax = tro.id_releve_occtax
	LEFT JOIN pr_occtax.cor_counting_occtax cco ON cco.id_occurrence_occtax = too.id_occurrence_occtax
WITH NO DATA ;


\echo '-------------------------------------------------------------------------------'
\echo 'Add primary key on imports occtax table'
\set importTablePk 'pk_':occtaxImportTable
ALTER TABLE gn_imports.:occtaxImportTable
	ALTER COLUMN gid ADD GENERATED ALWAYS AS IDENTITY,
	ADD CONSTRAINT :importTablePk PRIMARY KEY(gid);


\echo '-------------------------------------------------------------------------------'
\echo 'Create indexes on imports occtax table'
\set uniqueIdIdx 'idx_unique_':occtaxImportTable'_unique_id_sinp_grp'
CREATE UNIQUE INDEX :uniqueIdIdx
    ON gn_imports.:occtaxImportTable USING btree (unique_id_sinp_grp);

\set updateDateIdx 'idx_':occtaxImportTable'_meta_update_date'
CREATE INDEX :updateDateIdx
    ON gn_imports.:occtaxImportTable USING btree (meta_update_date);

\set lastActionIdx 'idx_':occtaxImportTable'_meta_last_action'
CREATE INDEX :lastActionIdx
    ON gn_imports.:occtaxImportTable USING btree (meta_last_action);


\echo '-------------------------------------------------------------------------------'
\echo 'Attribute imports occtax to GeoNature DB owner'
ALTER TABLE gn_imports.:occtaxImportTable OWNER TO :gnDbOwner ;


\echo '-------------------------------------------------------------------------------'
\echo 'Copy CVS file to import occtax table'
COPY gn_imports.:occtaxImportTable (
    unique_id_sinp_grp,
    code_dataset,
    code_digitiser,
    observers,
    code_nomenclature_tech_collect_campanule,
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
    unique_id_occurence_occtax,
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
    unique_id_sinp_occtax,
    code_nomenclature_life_stage,
    code_nomenclature_sex,
    code_nomenclature_obj_count,
    code_nomenclature_type_count,
    count_min,
    count_max,
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
