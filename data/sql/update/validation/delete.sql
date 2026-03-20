-- This file contain a variable "${validationImportTable}"" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Delete imported validation with meta_last_action = D.'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Batch deletion in "validation" of the imported observations'
DO $$
DECLARE
    step INTEGER ;
    stopAt INTEGER ;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER ;
    startTime TIMESTAMP ;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${validationImportTable}', 'D') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to delete in "validation" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to delete % validations from %', step, offsetCnt ;

        startTime := clock_timestamp();

        WITH validations_to_delete AS (
            SELECT unique_id_sinp::uuid AS unique_id
            FROM gn_imports.${validationImportTable}
            WHERE meta_last_action = 'D'
            ORDER BY gid ASC
            LIMIT step
            OFFSET offsetCnt
        )
        DELETE FROM ONLY gn_commons.t_validations AS v
        USING validations_to_delete AS vtd
        WHERE v.uuid_attached_row = vtd.unique_id ;

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Removed validations rows: %', affectedRows ;
        RAISE NOTICE 'Loop execution time: %', clock_timestamp() - startTime;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;
