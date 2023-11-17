
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
        cd_nom AS sciname_code,
        id_statut AS biogeographic_status_code,
        id_habitat AS habitat_type_code,
        id_rang AS rank_code,
        regne AS kingdom,
        phylum,
        classe AS "class",
        ordre AS "order",
        famille AS family,
        sous_famille AS subfamily,
        tribu AS tribe,
        cd_taxsup AS higher_taxon_code_short,
        cd_sup AS higher_taxon_code_full,
        cd_ref AS taxon_code,
        lb_nom AS sciname_short,
        lb_auteur AS sciname_author,
        nom_complet AS sciname,
        nom_complet_html AS sciname_html,
        nom_valide AS sciname_valid,
        nom_vern AS vernacular_name,
        nom_vern_eng AS vernacular_name_en,
        group1_inpn AS inpn_group1_label,
        group2_inpn AS inpn_group2_label,
        group3_inpn AS inpn_group3_label,
        "url" AS inpn_url,
        NULL::JSONB AS additional_data,
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
\set scinameCodeIdx 'idx_unique_':taxrefImportTable'_sciname_code'
CREATE UNIQUE INDEX :scinameCodeIdx
    ON gn_imports.:taxrefImportTable USING btree (sciname_code);

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
    sciname_code,
    biogeographic_status_code,
    habitat_type_code,
    rank_code,
    kingdom,
    phylum,
    "class",
    "order",
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
    inpn_url,
    additional_data,
    meta_create_date,
    meta_update_date,
    meta_last_action
)
FROM :'csvFilePath'
WITH (
    FORMAT CSV, HEADER, DELIMITER E'\t',
    FORCE_NULL (phylum, "class", "order", family, subfamily, tribe, sciname_short, sciname_valid, vernacular_name, vernacular_name_en)
);

\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
