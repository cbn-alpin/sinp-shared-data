BEGIN;
-- This file contain a variable "${occtaxImportTable}" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Insert imported occtax with meta_last_action = I.'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';

\echo '-------------------------------------------------------------------------------'
\echo 'Batch updating in "t_releves_occtax, t_occurrences_occtax, cor_counting_occtax" of the imported occtax'
DO $$
DECLARE
    step INTEGER ;
    stopAt INTEGER ;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${occtaxImportTable}', 'I') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to insert in "t_releves_occtax, t_occurrences_occtax, cor_counting_occtax" tables' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to insert % occtax from %', step, offsetCnt ;

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Inserting occtax data to "t_releves_occtax" if not exist' ;
        INSERT INTO pr_occtax.t_releves_occtax (
            unique_id_sinp_grp,
            id_dataset,
            id_digitiser,
            observers_txt,
--          id_nomenclature_tech_collect_campanule,
            id_nomenclature_grp_typ,
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
            comment,
            geom_local,
            geom_4326,
            id_nomenclature_geo_object_nature,
            precision,
            additional_fields,
            id_module
        )
        SELECT
            unique_id_sinp_grp,
            code_dataset,
            code_digitiser,
            observers,
--          code_nomenclature_tech_collect_campanule,
            code_nomenclature_grp_typ,
            grp_method,
            date_min::DATE,
            date_max::DATE,
            date_min::TIME,
            date_max::TIME,
            cd_hab,
            altitude_min,
            altitude_max,
            depth_min,
            depth_max,
            place_name,
            meta_device_entry,
            comment_context,
            geom,
            ST_Transform(geom, 4326),
            code_nomenclature_geo_object_nature,
            precision,
            additional_fields,
            gn_commons.get_id_module_by_code('OCCTAX')
        FROM gn_imports.${occtaxImportTable} AS ocit
        WHERE NOT EXISTS (
                SELECT 'X'
                FROM pr_occtax.t_releves_occtax AS tro
                WHERE tro.unique_id_sinp_grp = ocit.unique_id_sinp_grp
            )
            AND ocit.meta_last_action = 'I'
        ORDER BY ocit.gid ASC
        -- With NOT EXISTS don't use OFFSET because it's eliminate previously inserted rows.
        -- OFFSET offsetCnt
        LIMIT step ;
        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted releves occtax rows: %', affectedRows ;

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Inserting occurrences occtax data to "t_occurences_occtax" if not exist' ;
        INSERT INTO pr_occtax.t_occurrences_occtax (
            unique_id_occurence_occtax,
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
            meta_v_taxref,
            sample_number_proof,
            digital_proof,
            non_digital_proof,
            comment,
            additional_fields,
            id_releve_occtax
        )
        SELECT
            ocit.unique_id_occurence_occtax,
            ocit.code_nomenclature_obs_technique,
            ocit.code_nomenclature_bio_condition,
            ocit.code_nomenclature_bio_status,
            ocit.code_nomenclature_naturalness,
            ocit.code_nomenclature_exist_proof,
            ocit.code_nomenclature_diffusion_level,
            ocit.code_nomenclature_observation_status,
            ocit.code_nomenclature_blurring,
            ocit.code_nomenclature_source_status,
            ocit.code_nomenclature_behaviour,
            ocit.determiner,
            ocit.code_nomenclature_determination_method,
            ocit.cd_nom,
            ocit.nom_cite,
            ocit.meta_v_taxref,
            NULL,
            ocit.digital_proof,
            ocit.non_digital_proof,
            ocit.comment_description,
            ocit.additional_fields,
            relv.id_releve_occtax
        FROM gn_imports.${occtaxImportTable} AS ocit
            LEFT JOIN pr_occtax.t_releves_occtax AS relv
                ON relv.unique_id_sinp_grp = ocit.unique_id_sinp_grp
        WHERE NOT EXISTS (
                SELECT 'X'
                FROM pr_occtax.t_occurrences_occtax AS too
                WHERE too.unique_id_occurence_occtax = ocit.unique_id_occurence_occtax
            )
            AND ocit.meta_last_action = 'I'
        ORDER BY ocit.gid ASC
        -- With NOT EXISTS don't use OFFSET because it's eliminate previously inserted rows.
        -- OFFSET offsetCnt
        LIMIT step ;
        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted occurrences occtax rows: %', affectedRows ;

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Inserting counting occtax data to "cor_counting_occtax" if not exist' ;
        INSERT INTO pr_occtax.cor_counting_occtax (
            unique_id_sinp_occtax,
            id_nomenclature_life_stage,
            id_nomenclature_sex,
            id_nomenclature_obj_count,
            id_nomenclature_type_count,
            count_min,
            count_max,
            additional_fields,
            id_occurrence_occtax
        )
        SELECT
            ocit.unique_id_sinp_occtax,
            ocit.code_nomenclature_life_stage,
            ocit.code_nomenclature_sex,
            ocit.code_nomenclature_obj_count,
            ocit.code_nomenclature_type_count,
            ocit.count_min,
            ocit.count_max,
            ocit.additional_fields,
            occ.id_occurrence_occtax
        FROM gn_imports.${occtaxImportTable} AS ocit
            LEFT JOIN pr_occtax.t_occurrences_occtax AS occ
                ON occ.unique_id_occurence_occtax = ocit.unique_id_occurence_occtax
        WHERE NOT EXISTS (
                SELECT 'X'
                FROM pr_occtax.cor_counting_occtax AS cco
                WHERE cco.unique_id_sinp_occtax = ocit.unique_id_sinp_occtax
            )
            AND ocit.meta_last_action = 'I'
        ORDER BY ocit.gid ASC
        -- With NOT EXISTS don't use OFFSET because it's eliminate previously inserted rows.
        -- OFFSET offsetCnt
        LIMIT step ;
        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted counting occtax rows: %', affectedRows ;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;

\echo '----------------------------------------------------------------------------'
\echo 'Links datasets to module Occtax'
-- Enables selection of datasets on module Occtax for data entry tests
-- TODO: Remove this part after CBNA data is integrated into Occtax
INSERT INTO gn_commons.cor_module_dataset (
    id_module,
    id_dataset
)
    SELECT
        gn_commons.get_id_module_bycode('OCCTAX'),
        d.id_dataset
    FROM gn_meta.t_datasets AS d
        JOIN gn_meta.cor_dataset_actor AS cda
            ON d.id_dataset = cda.id_dataset
    WHERE d."active" = TRUE
        AND cda.id_nomenclature_actor_role = ref_nomenclatures.get_id_nomenclature('ROLE_ACTEUR', '6') -- Producteur
        AND cda.id_organism = utilisateurs.get_id_organism_by_uuid('5a433bd0-1fc0-25d9-e053-2614a8c026f8') -- Conservatoire botanique national alpin;
ON CONFLICT DO NOTHING ;

\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;