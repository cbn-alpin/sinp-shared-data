BEGIN;
-- This file contain a variable "${ooImportTable}" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Insert imported occurrences occtax with meta_last_action = I.'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';

\echo '-------------------------------------------------------------------------------'
\echo 'Batch updating in "t_occurrences_occtax" of the imported occurrences occtax'
DO $$
DECLARE
    step INTEGER ;
    stopAt INTEGER ;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${ooImportTable}', 'I') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to insert in "t_occurrences_occtax" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to insert % occurrences occtax from %', step, offsetCnt ;

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
            meta_v_taref,
            sample_number_proof,
            digital_proof,
            non_digital_proof,
            comment,
            additional_fields,
            meta_create_date,
            meta_update_date,
            meta_last_action
        )
        SELECT
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
        FROM gn_imports.${ooImportTable} AS ooit
        WHERE NOT EXISTS (
                SELECT 'X'
                FROM pr_occtax.t_occurrences_occtax AS too
                WHERE too.unique_id_occurence_occtax = ooit.unique_id_sinp_occtax
            )
            AND ooit.meta_last_action = 'I'
        ORDER BY ooit.gid ASC
        -- With NOT EXISTS don't use OFFSET because it's eliminate previously inserted rows.
        -- OFFSET offsetCnt
        LIMIT step ;

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted PARENT occurrences occtax rows: %', affectedRows ;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;

\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
