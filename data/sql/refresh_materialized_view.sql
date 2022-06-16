-- Refresh materialized view depending on Synthese.
--
-- Required rights: DB OWNER
-- GeoNature database compatibility : v2.6.2+
-- Transfert this script on server this way:
-- rsync -av ./refresh_materialized_view.sql geonat@db-<region>-sinp:~/data/shared/data/sql/ --dry-run
-- Use this script this way: psql -h localhost -U geonatadmin -d geonature2db \
--      -f ~/data/shared/data/sql/refresh_materialized_view.sql.sql
--
-- WARNING : this script is not used for now in imports bash script !
BEGIN;


\echo '----------------------------------------------------------------------------'
\echo 'Refresh "gn_synthese.v_synthese_for_export" materialized view if necessary'
DO $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM pg_matviews
            WHERE schemaname = 'gn_synthese' AND matviewname = 'v_synthese_for_export'
        ) IS TRUE THEN
            RAISE NOTICE ' "gn_synthese.v_synthese_for_export" is a materialized view => refresh !' ;
            REFRESH MATERIALIZED VIEW CONCURRENTLY gn_synthese.v_synthese_for_export ;
        ELSE
            RAISE NOTICE ' "gn_synthese.v_synthese_for_export" is not a materialized view => NO refresh !' ;
        END IF ;
    END
$$ ;


\echo '----------------------------------------------------------------------------'
\echo 'Refresh "gn_profiles" materialized view if necessary'
DO $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM information_schema.schemata
            WHERE schema_name = 'gn_profiles'
        ) IS TRUE THEN
            RAISE NOTICE ' Refreshing "gn_profiles" materialized views.' ;
            PERFORM gn_profiles.refresh_profiles();
        ELSE
            RAISE NOTICE ' "gn_profiles" schema not exists !' ;
        END IF ;
    END
$$ ;


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is OK:'
COMMIT;
