BEGIN;
-- This file contain a variable "${syntheseImportTable}" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Insert imported observations with meta_last_action = I.'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Disable trigger "tri_meta_dates_change_synthese"'
ALTER TABLE gn_synthese.synthese DISABLE TRIGGER tri_meta_dates_change_synthese ;


\echo '-------------------------------------------------------------------------------'
\echo 'Disable trigger "tri_insert_calculate_sensitivity"'
ALTER TABLE gn_synthese.synthese DISABLE TRIGGER tri_insert_calculate_sensitivity ;


\echo '-------------------------------------------------------------------------------'
\echo 'Disable trigger "tri_insert_cor_area_synthese"'
ALTER TABLE gn_synthese.synthese DISABLE TRIGGER tri_insert_cor_area_synthese ;


\echo '-------------------------------------------------------------------------------'
\echo 'Batch updating in "synthese" of the imported observations'
-- TODO: check if we can use source_key and entity_source_pk_value to link observations in NOT EXISTS
DO $$
DECLARE
    step INTEGER;
    stopAt INTEGER;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${syntheseImportTable}', 'I') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to insert in "synthese" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to insert % observations from %', step, offsetCnt ;

        INSERT INTO gn_synthese.synthese (
            unique_id_sinp,
            unique_id_sinp_grp,
            entity_source_pk_value,
            id_source,
            id_dataset,
            id_module,
            id_nomenclature_geo_object_nature,
            id_nomenclature_grp_typ,
            grp_method,
            id_nomenclature_obs_technique,
            id_nomenclature_bio_status,
            id_nomenclature_bio_condition,
            id_nomenclature_naturalness,
            id_nomenclature_exist_proof,
            id_nomenclature_valid_status,
            id_nomenclature_diffusion_level,
            id_nomenclature_life_stage,
            id_nomenclature_sex,
            id_nomenclature_obj_count,
            id_nomenclature_type_count,
            id_nomenclature_sensitivity,
            id_nomenclature_observation_status,
            id_nomenclature_blurring,
            id_nomenclature_source_status,
            id_nomenclature_info_geo_type,
            id_nomenclature_behaviour,
            id_nomenclature_biogeo_status,
            reference_biblio,
            count_min,
            count_max,
            cd_nom,
            cd_hab,
            nom_cite,
            meta_v_taxref,
            sample_number_proof,
            digital_proof,
            non_digital_proof,
            altitude_min,
            altitude_max,
            depth_min,
            depth_max,
            place_name,
            the_geom_4326,
            the_geom_point,
            the_geom_local,
            precision,
            id_area_attachment,
            date_min,
            date_max,
            validator,
            validation_comment,
            meta_validation_date,
            observers,
            determiner,
            id_digitiser,
            id_nomenclature_determination_method,
            comment_context,
            comment_description,
            additional_data,
            meta_create_date,
            meta_update_date,
            last_action
        )
        SELECT
            unique_id_sinp,
            unique_id_sinp_grp,
            source_key,
            source_id,
            dataset_id,
            module_id,
            id_nomenclature_geo_object_nature,
            id_nomenclature_grp_typ,
            grp_method,
            id_nomenclature_obs_technique,
            id_nomenclature_bio_status,
            id_nomenclature_bio_condition,
            id_nomenclature_naturalness,
            id_nomenclature_exist_proof,
            id_nomenclature_valid_status,
            id_nomenclature_diffusion_level,
            id_nomenclature_life_stage,
            id_nomenclature_sex,
            id_nomenclature_obj_count,
            id_nomenclature_type_count,
            id_nomenclature_sensitivity,
            id_nomenclature_observation_status,
            id_nomenclature_blurring,
            id_nomenclature_source_status,
            id_nomenclature_info_geo_type,
            id_nomenclature_behaviour,
            id_nomenclature_biogeo_status,
            reference_biblio,
            count_min,
            count_max,
            cd_nom,
            cd_hab,
            nom_cite,
            meta_v_taxref,
            sample_number_proof,
            digital_proof,
            non_digital_proof,
            altitude_min,
            altitude_max,
            depth_min,
            depth_max,
            place_name,
            ST_Transform(geom, 4326),
            ST_Transform(ST_Centroid(geom), 4326),
            geom,
            precision,
            id_area_attachment,
            date_min,
            date_max,
            validator,
            validation_comment,
            validation_date,
            gn_imports.clean_observers_uuid(observers),
            determiner,
            id_digitiser,
            id_nomenclature_determination_method,
            comment_context,
            comment_description,
            additional_data,
            meta_create_date,
            meta_update_date,
            meta_last_action
        FROM gn_imports.${syntheseImportTable} AS sit
        WHERE NOT EXISTS (
                SELECT 'X'
                FROM gn_synthese.synthese AS s
                WHERE s.unique_id_sinp = sit.unique_id_sinp
            )
            AND sit.meta_last_action = 'I'
        ORDER BY sit.gid ASC
        -- With NOT EXISTS don't use OFFSET because it's eliminate previously inserted rows.
        -- OFFSET offsetCnt
        LIMIT step ;

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted synthese rows: %', affectedRows ;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Enable trigger "tri_insert_cor_area_synthese"'
ALTER TABLE gn_synthese.synthese ENABLE TRIGGER tri_insert_cor_area_synthese ;


\echo '-------------------------------------------------------------------------------'
\echo 'Enable trigger "tri_meta_dates_change_synthese"'
ALTER TABLE gn_synthese.synthese ENABLE TRIGGER tri_meta_dates_change_synthese ;


\echo '-------------------------------------------------------------------------------'
\echo 'Enable trigger "tri_insert_calculate_sensitivity"'
ALTER TABLE gn_synthese.synthese ENABLE TRIGGER tri_insert_calculate_sensitivity ;


\echo '-------------------------------------------------------------------------------'
\echo 'Delete temporary table "observers_uuid"'
DROP TABLE IF EXISTS gn_imports.observers_uuid;


\echo '-------------------------------------------------------------------------------'
\echo 'Create temporary table "observers_uuid"'
CREATE TABLE gn_imports.observers_uuid (
    gid SERIAL CONSTRAINT pk_observers_uuid PRIMARY KEY,
    observation_uuid UUID NOT NULL,
    observation_id INT,
    observer_uuid UUID NOT NULL,
    observer_id INT
);

\echo '-------------------------------------------------------------------------------'
\echo 'Insert data in temporary table "observers_uuid"'
INSERT INTO gn_imports.observers_uuid (observation_uuid, observer_uuid)
    SELECT
        unique_id_sinp AS observation_uuid,
        UNNEST(gn_imports.extract_observers_uuid(observers)) AS observer_uuid
    FROM gn_imports.${syntheseImportTable} ;


\echo '-------------------------------------------------------------------------------'
\echo 'Create infexes for temporary table "observers_uuid"'
CREATE INDEX "observers_observations_uuid_idx" ON gn_imports.observers_uuid USING btree(observation_uuid);
CREATE INDEX "observers_observer_uuid_idx" ON gn_imports.observers_uuid USING btree(observer_uuid);


\echo '-------------------------------------------------------------------------------'
\echo 'Update observation_id for temporary table "observers_uuid"'
UPDATE gn_imports.observers_uuid AS ou SET
    observation_id = s.id_synthese
FROM gn_synthese.synthese AS s
WHERE s.unique_id_sinp = ou.observation_uuid ;


\echo '-------------------------------------------------------------------------------'
\echo 'Update observer_id for temporary table "observers_uuid"'
UPDATE gn_imports.observers_uuid AS ou SET
    observer_id = r.id_role
FROM utilisateurs.t_roles AS r
WHERE r.uuid_role = ou.observer_uuid ;


\echo '-------------------------------------------------------------------------------'
\echo 'Insert data into cor_observer_synthese'
INSERT INTO gn_synthese.cor_observer_synthese (id_synthese, id_role)
    SELECT
        observation_id,
        observer_id
    FROM gn_imports.observers_uuid
ON CONFLICT ON CONSTRAINT pk_cor_observer_synthese DO NOTHING ;


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
