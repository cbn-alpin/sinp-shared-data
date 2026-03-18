BEGIN;
-- This file contain a variable "${validationImportTable}" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Update imported validations with meta_last_action = U.'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Disable trigger "tri_insert_synthese_update_validation_status"'
ALTER TABLE gn_commons.t_validations DISABLE TRIGGER tri_insert_synthese_update_validation_status ;


\echo '-------------------------------------------------------------------------------'
\echo 'Batch updating in "validation" of the imported validations'
DO $$
DECLARE
    step INTEGER ;
    stopAt INTEGER ;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER ;
    startTime TIMESTAMP ;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${validationImportTable}', 'U') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to update in "validation" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to update % observations from %', step, offsetCnt ;

        startTime := clock_timestamp();

        UPDATE gn_commons.t_validations AS v SET
            uuid_attached_row = vit.unique_id_sinp,
            id_nomenclature_valid_status = vit.id_nomenclature_valid_status,
            validation_auto = vit."automatic",
            id_validator = utilisateurs.get_one_role_id_from_uuid_in_string(vit.validator),
            validation_comment = gn_commons.format_validation_comment(vit.comment, vit.validator),
            validation_date = vit.creation_date,
            additional_data = vit.additional_data,
            meta_create_date = vit.meta_create_date,
            meta_update_date = vit.meta_update_date,
            last_action = vit.meta_last_action
        FROM (
            SELECT
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
            FROM gn_imports.${validationImportTable}
            WHERE meta_last_action = 'U'
            ORDER BY gid ASC
            LIMIT step
            OFFSET offsetCnt
        ) AS vit
        WHERE vit.unique_id_sinp = v.uuid_attached_row
            AND vit.creation_date = v.validation_date ;

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Updated validation rows: %', affectedRows ;
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
