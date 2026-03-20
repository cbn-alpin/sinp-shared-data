BEGIN;
-- This file contain a variable "${syntheseImportTable}" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Update imported observations with meta_last_action = U.'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Disable trigger "tri_update_calculate_sensitivity"'
ALTER TABLE gn_synthese.synthese DISABLE TRIGGER tri_update_calculate_sensitivity ;


\echo '-------------------------------------------------------------------------------'
\echo 'Disable trigger "tri_update_cor_area_synthese"'
ALTER TABLE gn_synthese.synthese DISABLE TRIGGER tri_update_cor_area_synthese ;


\echo '-------------------------------------------------------------------------------'
\echo 'Batch updating in "synthese" of the imported observations'
DO $$
DECLARE
    step INTEGER ;
    stopAt INTEGER ;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER ;
    startTime TIMESTAMP ;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${syntheseImportTable}', 'U') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to update in "synthese" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to update % observations from %', step, offsetCnt ;

        startTime := clock_timestamp();

        UPDATE gn_synthese.synthese AS s SET
            unique_id_sinp = sit.unique_id_sinp,
            unique_id_sinp_grp = sit.unique_id_sinp_grp,
            entity_source_pk_value = sit.source_key,
            id_source = sit.source_id,
            id_dataset = sit.dataset_id,
            id_module = sit.module_id,
            id_nomenclature_geo_object_nature = sit.id_nomenclature_geo_object_nature,
            id_nomenclature_grp_typ = sit.id_nomenclature_grp_typ,
            grp_method = sit.grp_method,
            id_nomenclature_obs_technique = sit.id_nomenclature_obs_technique,
            id_nomenclature_bio_status = sit.id_nomenclature_bio_status,
            id_nomenclature_bio_condition = sit.id_nomenclature_bio_condition,
            id_nomenclature_naturalness = sit.id_nomenclature_naturalness,
            id_nomenclature_exist_proof = sit.id_nomenclature_exist_proof,
            id_nomenclature_valid_status = sit.id_nomenclature_valid_status,
            id_nomenclature_diffusion_level = sit.id_nomenclature_diffusion_level,
            id_nomenclature_life_stage = sit.id_nomenclature_life_stage,
            id_nomenclature_sex = sit.id_nomenclature_sex,
            id_nomenclature_obj_count = sit.id_nomenclature_obj_count,
            id_nomenclature_type_count = sit.id_nomenclature_type_count,
            id_nomenclature_sensitivity = sit.id_nomenclature_sensitivity,
            id_nomenclature_observation_status = sit.id_nomenclature_observation_status,
            id_nomenclature_blurring = sit.id_nomenclature_blurring,
            id_nomenclature_source_status = sit.id_nomenclature_source_status,
            id_nomenclature_info_geo_type = sit.id_nomenclature_info_geo_type,
            id_nomenclature_behaviour = sit.id_nomenclature_behaviour,
            id_nomenclature_biogeo_status = sit.id_nomenclature_biogeo_status,
            reference_biblio = sit.reference_biblio,
            count_min = sit.count_min,
            count_max = sit.count_max,
            cd_nom = sit.cd_nom,
            cd_hab = sit.cd_hab,
            nom_cite = sit.nom_cite,
            meta_v_taxref = sit.meta_v_taxref,
            sample_number_proof = sit.sample_number_proof,
            digital_proof = sit.digital_proof,
            non_digital_proof = sit.non_digital_proof,
            altitude_min = sit.altitude_min,
            altitude_max = sit.altitude_max,
            depth_min = sit.depth_min,
            depth_max = sit.depth_max,
            place_name = sit.place_name,
            the_geom_4326 = ST_Transform(sit.geom, 4326),
            the_geom_point = ST_Transform(ST_Centroid(sit.geom), 4326),
            the_geom_local = sit.geom,
            precision = sit.precision,
            date_min = sit.date_min,
            date_max = sit.date_max,
            validator = sit.validator,
            validation_comment = sit.validation_comment,
            meta_validation_date = sit.validation_date,
            observers = sit.observers,
            determiner = sit.determiner,
            id_digitiser = sit.id_digitiser,
            id_nomenclature_determination_method = sit.id_nomenclature_determination_method,
            comment_context = sit.comment_context,
            comment_description = sit.comment_description,
            additional_data = sit.additional_data,
            meta_create_date = sit.meta_create_date,
            meta_update_date = sit.meta_update_date,
            last_action = sit.meta_last_action
        FROM (
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
                geom,
                precision,
                date_min,
                date_max,
                gn_imports.clean_observers_uuid(validator) AS validator,
                validation_comment,
                validation_date,
                gn_imports.clean_observers_uuid(observers) AS observers,
                determiner,
                id_digitiser,
                id_nomenclature_determination_method,
                comment_context,
                comment_description,
                additional_data,
                meta_create_date,
                meta_update_date,
                meta_last_action
            FROM gn_imports.${syntheseImportTable}
            WHERE meta_last_action = 'U'
            ORDER BY gid ASC
            LIMIT step
            OFFSET offsetCnt
        ) AS sit
        WHERE sit.unique_id_sinp = s.unique_id_sinp ;
            -- Avoid use of source_key (with index or not), the query is very slow !
            -- OR sit.source_key = s.entity_source_pk_value ;
            -- Avoid using meta_update_date because it's not always correct.
            -- AND sit.meta_update_date > s.meta_update_date ;

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Updated synthese rows: %', affectedRows ;
        RAISE NOTICE 'Loop execution time: %', clock_timestamp() - startTime;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Enable trigger "tri_update_cor_area_synthese"'
ALTER TABLE gn_synthese.synthese ENABLE TRIGGER tri_update_cor_area_synthese ;


\echo '-------------------------------------------------------------------------------'
\echo 'Enable trigger "tri_update_calculate_sensitivity"'
ALTER TABLE gn_synthese.synthese ENABLE TRIGGER tri_update_calculate_sensitivity ;


\echo '-------------------------------------------------------------------------------'
\echo 'Disable triggers on cor_observer_synthese'
ALTER TABLE gn_synthese.cor_observer_synthese DISABLE TRIGGER trg_maj_synthese_observers_txt;

ALTER TABLE gn_synthese.cor_observer_synthese DROP CONSTRAINT IF EXISTS fk_gn_synthese_id_role ;

ALTER TABLE gn_synthese.cor_observer_synthese DROP CONSTRAINT IF EXISTS fk_gn_synthese_id_synthese ;


\echo '-------------------------------------------------------------------------------'
\echo 'Clean previous observers for updated observations'
DELETE FROM gn_synthese.cor_observer_synthese
WHERE id_synthese IN (
    SELECT s.id_synthese
    FROM gn_imports.${syntheseImportTable} AS sit
    JOIN gn_synthese.synthese AS s ON s.unique_id_sinp = sit.unique_id_sinp
    WHERE sit.meta_last_action = 'U'
);


\echo '-------------------------------------------------------------------------------'
\echo 'Insert new observers for updated observations'
INSERT INTO gn_synthese.cor_observer_synthese (id_synthese, id_role)
    WITH observers_uuid AS (
        SELECT
            unique_id_sinp AS observation_uuid,
            UNNEST(regexp_matches(observers, '\[([-0-9a-fA-F]{36})\]', 'g'))::uuid AS observer_uuid
        FROM gn_imports.${syntheseImportTable}
        WHERE meta_last_action = 'U'
    )
    SELECT DISTINCT
        s.id_synthese,
        r.id_role
    FROM observers_uuid AS ou
        JOIN gn_synthese.synthese AS s
            ON s.unique_id_sinp = ou.observation_uuid
        JOIN utilisateurs.t_roles AS r
            ON r.uuid_role = ou.observer_uuid
ON CONFLICT ON CONSTRAINT pk_cor_observer_synthese DO NOTHING ;


\echo '----------------------------------------------------------------------------'
\echo 'Enable triggers on cor_observer_synthese'
ALTER TABLE gn_synthese.cor_observer_synthese ENABLE TRIGGER trg_maj_synthese_observers_txt;

ALTER TABLE gn_synthese.cor_observer_synthese ADD CONSTRAINT fk_gn_synthese_id_role
    FOREIGN KEY (id_role) REFERENCES utilisateurs.t_roles(id_role)
    ON UPDATE CASCADE ;

ALTER TABLE gn_synthese.cor_observer_synthese ADD CONSTRAINT fk_gn_synthese_id_synthese
    FOREIGN KEY (id_synthese) REFERENCES gn_synthese.synthese(id_synthese)
    ON UPDATE CASCADE ON DELETE CASCADE ;


\echo '----------------------------------------------------------------------------'
\echo 'Disable triggers on gn_commons.t_validations'
ALTER TABLE gn_commons.t_validations
    DISABLE TRIGGER tri_insert_synthese_update_validation_status ;


\echo '----------------------------------------------------------------------------'
\echo 'Add validations data to gn_commons.t_validations for updated observations'
WITH validations_to_upsert AS (
    SELECT
        unique_id_sinp::uuid AS uuid_attached_row,
        id_nomenclature_valid_status,
        gn_commons.determine_auto_validation(validator, validation_date, id_nomenclature_valid_status) AS validation_auto,
        utilisateurs.get_one_role_id_from_uuid_in_string(validator) AS id_validator,
        gn_commons.format_validation_comment(validation_comment, validator) AS validation_comment,
        validation_date
    FROM gn_imports.${syntheseImportTable}
    WHERE meta_last_action = 'U'
        AND id_nomenclature_valid_status IS NOT NULL
),
updated AS (
    UPDATE gn_commons.t_validations AS v SET
        id_nomenclature_valid_status = vtu.id_nomenclature_valid_status,
        id_validator = vtu.id_validator,
        validation_comment = vtu.validation_comment
    FROM validations_to_upsert AS vtu
    WHERE v.uuid_attached_row = vtu.uuid_attached_row
        AND v.validation_date = vtu.validation_date
    RETURNING v.uuid_attached_row, v.validation_date
),
inserted AS (
    INSERT INTO gn_commons.t_validations (
        uuid_attached_row,
        id_nomenclature_valid_status,
        validation_auto,
        id_validator,
        validation_comment,
        validation_date
    )
    SELECT
        vtu.uuid_attached_row,
        vtu.id_nomenclature_valid_status,
        vtu.validation_auto,
        vtu.id_validator,
        vtu.validation_comment,
        vtu.validation_date
    FROM validations_to_upsert AS vtu
        LEFT JOIN updated AS u
            ON vtu.uuid_attached_row = u.uuid_attached_row AND vtu.validation_date = u.validation_date
    WHERE u.uuid_attached_row IS NULL
    RETURNING uuid_attached_row
)
SELECT
    (SELECT count(*) FROM validations_to_upsert) AS validations_to_upsert_count,
    (SELECT count(*) FROM updated) AS updated_count,
    (SELECT count(*) FROM inserted) AS inserted_count ;


\echo '----------------------------------------------------------------------------'
\echo 'Enable triggers on gn_commons.t_validations'
ALTER TABLE gn_commons.t_validations
    ENABLE TRIGGER tri_insert_synthese_update_validation_status ;


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
