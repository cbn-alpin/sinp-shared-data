BEGIN;
-- This file contain a variable "${coImportTable}" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Insert imported counting occtax with meta_last_action = I.'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';

\echo '-------------------------------------------------------------------------------'
\echo 'Batch updating in "cor_counting_occtax" of the imported counting occtax'
DO $$
DECLARE
    step INTEGER ;
    stopAt INTEGER ;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${coImportTable}', 'I') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to insert in "cor_counting_occtax" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to insert % counting occtax from %', step, offsetCnt ;

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Inserting counting occtax data to "cor_counting_occtax" if not exist' ;
        INSERT INTO pr_occtax.cor_counting_occtax (
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
        SELECT
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
        FROM gn_imports.${coImportTable} AS coit
        WHERE NOT EXISTS (
                SELECT 'X'
                FROM pr_occtax.cor_counting_occtax AS cco
                WHERE cco.unique_id_sinp_occtax = coit.unique_id_sinp_occtax
            )
            AND coit.meta_last_action = 'I'
        ORDER BY coit.gid ASC
        -- With NOT EXISTS don't use OFFSET because it's eliminate previously inserted rows.
        -- OFFSET offsetCnt
        LIMIT step ;

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted PARENT couting occtax rows: %', affectedRows ;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;

\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
