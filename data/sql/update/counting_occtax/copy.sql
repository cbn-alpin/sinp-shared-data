BEGIN;

\echo '-------------------------------------------------------------------------------'
\echo 'Copy CSV data into gn_imports schema and counting occtax table.'
\echo 'Rights: superuser'
\echo 'GeoNature database compatibility : v2.4.1'

SET client_encoding = 'UTF8';


\echo '-------------------------------------------------------------------------------'
\echo 'Remove imports counting occtax table if already exists'
DROP TABLE IF EXISTS gn_imports.:coImportTable ;


\echo '-------------------------------------------------------------------------------'
\echo 'Create imports counting occtax table from "cor_counting_occtax" with additional fields'
CREATE TABLE gn_imports.:coImportTable AS
    SELECT
        NULL::INT AS gid,
        unique_id_sinp_occtax,
        code_nomenclature_life_stage,
        code_nomenclature_sex,
        code_nomenclature_obj_count,
        code_nomenclature_type_count,
        count_min,
        count_max,
        additional_fields,
        NULL::TIMESTAMP AS meta_create_date,
        NULL::TIMESTAMP AS meta_update_date,
        NULL::BPCHAR(1) AS meta_last_action
    FROM pr_occtax.cor_counting_occtax
WITH NO DATA ;


\echo '-------------------------------------------------------------------------------'
\echo 'Add primary key on imports counting occtax table'
\set importTablePk 'pk_':coImportTable
ALTER TABLE gn_imports.:coImportTable
	ALTER COLUMN gid ADD GENERATED ALWAYS AS IDENTITY,
	ADD CONSTRAINT :importTablePk PRIMARY KEY(gid);


\echo '-------------------------------------------------------------------------------'
\echo 'Create indexes on imports counting occtax table'
\set uniqueIdIdx 'idx_unique_':coImportTable'_unique_id_sinp_occtax'
CREATE UNIQUE INDEX :uniqueIdIdx
    ON gn_imports.:coImportTable USING btree (unique_id_sinp_occtax);

\set updateDateIdx 'idx_':coImportTable'_meta_update_date'
CREATE INDEX :updateDateIdx
    ON gn_imports.:coImportTable USING btree (meta_update_date);

\set lastActionIdx 'idx_':coImportTable'_meta_last_action'
CREATE INDEX :lastActionIdx
    ON gn_imports.:coImportTable USING btree (meta_last_action);


\echo '-------------------------------------------------------------------------------'
\echo 'Attribute imports counting occtax to GeoNature DB owner'
ALTER TABLE gn_imports.:coImportTable OWNER TO :gnDbOwner ;


\echo '-------------------------------------------------------------------------------'
\echo 'Copy CVS file to import counting occtax table'
COPY gn_imports.:coImportTable (
    unique_id_sinp_occtax,
    code_nomenclature_life_stage,
    code_nomenclature_sex,
    code_nomenclature_obj_count,
    code_nomenclature_type_count,
    count_min,
    count_max,
    additional_fields,
    meta_create_date,
    meta_update_date,
    meta_last_action
)
FROM :'csvFilePath'
WITH CSV HEADER DELIMITER E'\t' NULL '\N' ;


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
