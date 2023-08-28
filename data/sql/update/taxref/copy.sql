
BEGIN;

\echo '-------------------------------------------------------------------------------'
\echo 'Copy CSV data into gn_imports schema and taxref table.'
\echo 'Rights: superuser'
\echo 'GeoNature database compatibility : v2.4.1'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Remove imports taxref table if already exists'
DROP TABLE IF EXISTS gn_imports.:taxrefImportTable ;


\echo '-------------------------------------------------------------------------------'
\echo 'Create imports taxref table from "taxref" with additional fields'
CREATE TABLE gn_imports.:taxrefImportTable AS
    SELECT
        NULL::INT AS gid,
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
        group3_inpn,
        NULL::TIMESTAMP AS meta_create_date,
        NULL::TIMESTAMP AS meta_update_date,
        NULL::BPCHAR(1) AS meta_last_action
    FROM taxonomie.taxref
WITH NO DATA ;


\echo '-------------------------------------------------------------------------------'
\echo 'Add primary key on imports taxref table'
\set importTablePk 'pk_':taxrefImportTable
ALTER TABLE gn_imports.:taxrefImportTable
	ALTER COLUMN gid ADD GENERATED ALWAYS AS IDENTITY,
	ADD CONSTRAINT :importTablePk PRIMARY KEY(gid);


\echo '-------------------------------------------------------------------------------'
\echo 'Create indexes on imports taxref table'
\set cdNomIdx 'idx_unique_':taxrefImportTable'_cdNom'
CREATE UNIQUE INDEX :cdNomIdx
    ON gn_imports.:taxrefImportTable USING btree (cd_nom);

\set updateDateIdx 'idx_':taxrefImportTable'_meta_update_date'
CREATE INDEX :updateDateIdx
    ON gn_imports.:taxrefImportTable USING btree (meta_update_date);

\set lastActionIdx 'idx_':taxrefImportTable'_meta_last_action'
CREATE INDEX :lastActionIdx
    ON gn_imports.:taxrefImportTable USING btree (meta_last_action);


\echo '-------------------------------------------------------------------------------'
\echo 'Attribute imports taxref to GeoNature DB owner'
ALTER TABLE gn_imports.:taxrefImportTable OWNER TO :gnDbOwner ;


\echo '-------------------------------------------------------------------------------'
\echo 'Copy CSV file to import taxref table'
COPY gn_imports.:taxrefImportTable (
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
    group3_inpn,
    meta_create_date,
    meta_update_date,
    meta_last_action
)
FROM :'csvFilePath'
WITH (FORMAT CSV, HEADER, DELIMITER E'\t', FORCE_NULL (phylum, classe, ordre, famille, sous_famille, tribu, lb_auteur, nom_valide, nom_vern, nom_vern_eng));


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
