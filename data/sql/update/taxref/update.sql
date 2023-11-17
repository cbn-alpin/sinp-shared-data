BEGIN;
-- This file contain a variable "${taxrefImportTable}" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Update imported taxref with meta_last_action = U.'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Batch updating in "taxref" of the imported taxref'
DO $$
DECLARE
    step INTEGER ;
    stopAt INTEGER ;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${taxrefImportTable}', 'U') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to update in "taxref" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to update % taxrefs from %', step, offsetCnt ;

        UPDATE taxonomie.taxref AS t SET
            cd_nom = tit.sciname_code,
            id_statut = tit.biogeographic_status_code,
            id_habitat = tit.habitat_type_code,
            id_rang = tit.rank_code,
            regne = tit.rekingdomgne,
            phylum = tit.phylum,
            classe = tit.class,
            ordre = tit.order,
            famille = tit.family,
            sous_famille = tit.subfamily,
            tribu = tit.tribe,
            cd_taxsup = tit.higher_taxon_code_short,
            cd_sup = tit.higher_taxon_code_full,
            cd_ref = tit.taxon_code,
            lb_nom = tit.sciname_short,
            lb_auteur = tit.sciname_author,
            nom_complet = tit.sciname,
            nom_complet_html = tit.sciname_html,
            nom_valide = tit.sciname_valid,
            nom_vern = tit.vernacular_name,
            nom_vern_eng = tit.vernacular_name_en,
            group1_inpn = tit.inpn_group1_label,
            group2_inpn = tit.inpn_group2_label,
            group3_inpn = tit.inpn_group3_label,
            "url" = tit.inpn_url
        FROM (
            SELECT
                sciname_code,
                biogeographic_status_code,
                habitat_type_code,
                rank_code,
                kingdom,
                phylum,
                class,
                order,
                family,
                subfamily,
                tribe,
                higher_taxon_code_short,
                higher_taxon_code_full,
                taxon_code,
                sciname_short,
                sciname_author,
                sciname,
                sciname_html,
                sciname_valid,
                vernacular_name,
                vernacular_name_en,
                inpn_group1_label,
                inpn_group2_label,
                inpn_group3_label,
                inpn_url
            FROM gn_imports.${taxrefImportTable}
            WHERE meta_last_action = 'U'
            ORDER BY gid ASC
            LIMIT step
            OFFSET offsetCnt
        ) AS tit
        WHERE tit.sciname_code = t.cd_nom
        ;

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Update affected rows: %', affectedRows ;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
