BEGIN;
-- This file contain a variable "${organismImportTable}" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Update imported organisms with meta_last_action = U.'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Batch updating in "bib_organismes" of the imported organisms'
DO $$
DECLARE
    step INTEGER ;
    stopAt INTEGER ;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${organismImportTable}', 'U') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to update in "bib_organismes" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to update % organisms from %', step, offsetCnt ;

        WITH organisms_to_update AS (
            SELECT
                unique_id,
                "name",
                "address",
                postal_code,
                city,
                phone,
                fax,
                email,
                organism_url,
                logo_url,
                additional_data
            FROM gn_imports.${organismImportTable}
            WHERE meta_last_action = 'U'
            ORDER BY gid ASC
            LIMIT step
            OFFSET offsetCnt
        )
        UPDATE utilisateurs.bib_organismes AS bo SET
            uuid_organisme = otu.unique_id,
            nom_organisme = otu.name,
            adresse_organisme = otu.address,
            cp_organisme = otu.postal_code,
            ville_organisme = otu.city,
            tel_organisme = otu.phone,
            fax_organisme = otu.fax,
            email_organisme = otu.email,
            url_organisme = otu.organism_url,
            url_logo = otu.logo_url,
            additional_data = otu.additional_data
        FROM organisms_to_update AS otu
        WHERE bo.uuid_organisme = otu.unique_id
            OR (
                bo.nom_organisme = otu.name
                AND utilisateurs.get_id_organism_by_uuid(otu.unique_id::uuid) IS NULL
            );
        -- GN v2.17 introduces unique index on 'nom_organisme', so we can update UUID on same name.
        -- On previous GN version clean your table and add unique index on 'nom_organisme'.

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Update affected rows: %', affectedRows ;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
