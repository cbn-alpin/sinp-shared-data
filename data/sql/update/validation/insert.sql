BEGIN;
-- This file contain a variable "${validationImportTable}" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Insert imported validations with meta_last_action = I.'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Disable trigger "tri_insert_synthese_update_validation_status"'
ALTER TABLE gn_commons.t_validations DISABLE TRIGGER tri_insert_synthese_update_validation_status ;


\echo '-------------------------------------------------------------------------------'
\echo 'Batch updating in "t_validations" of the imported validations'
DO $$
DECLARE
    step INTEGER ;
    stopAt INTEGER ;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER ;
    startTime TIMESTAMP ;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${validationImportTable}', 'I') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to insert in "t_validations" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to insert % validations from %', step, offsetCnt ;

        startTime := clock_timestamp();

        INSERT INTO gn_commons.t_validations (
            uuid_attached_row,
            id_nomenclature_valid_status,
            validation_auto,
            id_validator,
            validation_comment,
            validation_date
        )
        SELECT
            unique_id_sinp AS uuid_attached_row,
            id_nomenclature_valid_status,
            "automatic" AS validation_auto,
            utilisateurs.get_one_role_id_from_uuid_in_string(validator) AS id_validator,
            gn_commons.format_validation_comment(comment, validator) AS validation_comment,
            creation_date AS validation_date
        FROM gn_imports.${validationImportTable} AS vit
        WHERE NOT EXISTS (
                SELECT 'X'
                FROM gn_commons.t_validations AS v
                WHERE v.uuid_attached_row = vit.unique_id_sinp
                    AND v.validation_date = vit.creation_date
            )
            AND vit.meta_last_action = 'I'
        ORDER BY vit.gid ASC
        LIMIT step ;
        -- With NOT EXISTS don't use OFFSET because it's eliminate previously inserted rows.

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted validation rows: %', affectedRows ;
        RAISE NOTICE 'Loop execution time: %', clock_timestamp() - startTime;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Enable trigger "tri_insert_synthese_update_validation_status"'
ALTER TABLE gn_commons.t_validations ENABLE TRIGGER tri_insert_synthese_update_validation_status ;


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
