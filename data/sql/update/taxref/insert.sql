BEGIN;
-- This file contain a variable "${taxrefImportTable}"" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Insert to gn_imports.taxref imported taxref data with meta_last_action = I.'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Batch insertion of the taxref data imported into "taxref" if they do not exist'
DO $$
DECLARE
    step INTEGER ;
    stopAt INTEGER ;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${taxrefImportTable}', 'I') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to insert in "taxref" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to insert % taxrefs from %', step, offsetCnt ;

        INSERT INTO taxonomie.taxref(
            cd_nom,
            id_statut,
            id_habitat,
            id_rang,
            regne,
            phylum,
            classe,
            ordre,
            famille,
            sous_famille,
            tribu,
            cd_taxsup,
            cd_sup,
            cd_ref,
            lb_nom,
            lb_auteur,
            nom_complet,
            nom_complet_html,
            nom_valide,
            nom_vern,
            nom_vern_eng,
            group1_inpn,
            group2_inpn,
            "url",
            group3_inpn
        )
        SELECT
            cd_nom,
            id_statut,
            id_habitat,
            id_rang,
            regne,
            phylum,
            classe,
            ordre,
            famille,
            sous_famille,
            tribu,
            cd_taxsup,
            cd_sup,
            cd_ref,
            lb_nom,
            lb_auteur,
            nom_complet,
            nom_complet_html,
            nom_valide,
            nom_vern,
            nom_vern_eng,
            group1_inpn,
            group2_inpn,
            "url",
            group3_inpn
        FROM gn_imports.${taxrefImportTable} AS sit
        WHERE sit.meta_last_action = 'I'
            AND NOT EXISTS (
                SELECT 'X'
                FROM taxonomie.taxref AS ts
                WHERE ts.cd_nom = sit.cd_nom
            )
        ORDER BY sit.gid ASC
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

