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
            cd_nom = tit.cd_nom,
            id_statut = tit.id_statut,
            id_habitat = tit.id_habitat,
            id_rang = tit.id_rang,
            regne = tit.regne,
            phylum = tit.phylum,
            classe = tit.classe,
            ordre = tit.ordre,
            famille = tit.famille,
            sous_famille = tit.sous_famille,
            tribu = tit.tribu,
            cd_taxsup = tit.cd_taxsup,
            cd_sup = tit.cd_sup,
            cd_ref = tit.cd_ref,
            lb_nom = tit.lb_nom,
            lb_auteur = tit.lb_auteur,
            nom_complet = tit.nom_complet,
            nom_complet_html = tit.nom_complet_html,
            nom_valide = tit.nom_valide,
            nom_vern = tit.nom_vern,
            nom_vern_eng = tit.nom_vern_eng,
            group1_inpn = tit.group1_inpn,
            group2_inpn = tit.group2_inpn,
            "url" = tit.url,
            group3_inpn = tit.group3_inpn
        FROM (
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
            FROM gn_imports.${taxrefImportTable}
            WHERE meta_last_action = 'U'
            ORDER BY gid ASC
            LIMIT step
            OFFSET offsetCnt
        ) AS tit
        WHERE tit.cd_nom = t.cd_nom
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
