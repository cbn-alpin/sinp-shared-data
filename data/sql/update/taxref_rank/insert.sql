BEGIN;
-- This file contain a variable "${taxrefImportTable}"" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Insert of gn_imports.taxrefrang data with meta_last_action = I into "taxonomie.bib_taxref_rangs".'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Batch insertion of the taxrefrang data imported into "bib_taxref_rangs" if they do not exist'
DO $$
DECLARE
    step INTEGER ;
    stopAt INTEGER ;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${trImportTable}', 'I') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to insert in "bib_taxref_rangs" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to insert % bibtaxrefrangs from %', step, offsetCnt ;

        INSERT INTO taxonomie.bib_taxref_rangs(
            id_rang,
            nom_rang,
            nom_rang_en,
            tri_rang
        )
        SELECT
            id_rang,
            nom_rang,
            nom_rang_en,
            tri_rang
        FROM gn_imports.${trImportTable} AS btrit
        WHERE btrit.meta_last_action = 'I'
            AND NOT EXISTS (
                SELECT 'X'
                FROM taxonomie.bib_taxref_rangs AS btr
                WHERE btr.id_rang = btrit.id_rang
            )
        ORDER BY btrit.gid ASC
        -- With NOT EXISTS don't use OFFSET because it's eliminate previously inserted rows.
        -- OFFSET offsetCnt
        LIMIT step ;

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Insert affected rows: %', affectedRows ;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;

