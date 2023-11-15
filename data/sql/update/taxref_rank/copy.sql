
BEGIN;

\echo '-------------------------------------------------------------------------------'
\echo 'Copy CSV data into gn_imports schema and taxref_rank table.'
\echo 'Rights: superuser'
\echo 'GeoNature database compatibility : v2.4.1'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Remove imports taxref_rank table if already exists'
DROP TABLE IF EXISTS gn_imports.:trImportTable ;


\echo '-------------------------------------------------------------------------------'
\echo 'Create imports taxref_rank table from "bib_taxref_rangs" with additional fields'
CREATE TABLE gn_imports.:trImportTable AS
    SELECT
        NULL::INT AS gid,
        id_rang,
        nom_rang,
        nom_rang_en,
        tri_rang,
        NULL::TIMESTAMP AS meta_create_date,
        NULL::TIMESTAMP AS meta_update_date,
        NULL::BPCHAR(1) AS meta_last_action
    FROM taxonomie.bib_taxref_rangs
WITH NO DATA ;


\echo '-------------------------------------------------------------------------------'
\echo 'Add primary key on imports taxref_rank table'
\set importTablePk 'pk_':trImportTable
ALTER TABLE gn_imports.:trImportTable
	ALTER COLUMN gid ADD GENERATED ALWAYS AS IDENTITY,
	ADD CONSTRAINT :importTablePk PRIMARY KEY(gid);


\echo '-------------------------------------------------------------------------------'
\echo 'Attribute imports taxref_rank to GeoNature DB owner'
ALTER TABLE gn_imports.:trImportTable OWNER TO :gnDbOwner ;


\echo '-------------------------------------------------------------------------------'
\echo 'Copy CSV file to import taxref_rank table'
COPY gn_imports.:trImportTable (
    id_rang,
    nom_rang,
    nom_rang_en,
    tri_rang,
    meta_create_date,
    meta_update_date,
    meta_last_action
)
FROM :'csvFilePath'
WITH (FORMAT CSV, HEADER, DELIMITER E'\t', FORCE_NULL (nom_rang_en));


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
