BEGIN;
-- This file contain a variable "${roImportTable}" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Insert imported releves occtax with meta_last_action = I.'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';

\echo '-------------------------------------------------------------------------------'
\echo 'Batch updating in "t_releves_occtax" of the imported releves occtax'
DO $$
DECLARE
    step INTEGER ;
    stopAt INTEGER ;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${roImportTable}', 'I') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to insert in "t_releves_occtax" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to insert % releves occtax from %', step, offsetCnt ;

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Inserting releves occtax data to "t_releves_occtax" if not exist' ;
        INSERT INTO pr_occtax.t_releves_occtax (
            unique_id_sinp_grp,
            id_dataset,
            id_digitiser,
            observers_txt,
            id_nomenclature_tech_collect_campanule,
            id_nomenclature_grp_typ,
            grp_method,
            date_min,
            date_max,
            hour_min,
            hour_max,
            cd_hab,
            altitude_min,
            altitude_max,
            depth_min,
            depth_max,
            place_name,
            meta_device_entry,
            comment,
            geom_local,
            geom_4326,
            id_nomenclature_geo_object_nature,
            precision,
            additional_fields,
            meta_create_date,
            meta_update_date
        )
        SELECT
            unique_id_sinp_grp,
            code_dataset,
            code_digitiser,
            observers,
            code_nomenclature_obs_technique,
            code_nomenclature_grp_typ,
            grp_method,
            date_min,
            date_max,
            hour_min,
            hour_max,
            cd_hab,
            altitude_min,
            altitude_max,
            depth_min,
            depth_max,
            place_name,
            meta_device_entry,
            comment_context,
            ST_Transform(geom, 4326),
            geom,
            code_nomenclature_geo_object_nature,
            precision,
            additional_fields,
            meta_create_date,
            meta_update_date,
            meta_last_action
        FROM gn_imports.${roImportTable} AS roit
        WHERE NOT EXISTS (
                SELECT 'X'
                FROM pr_occtax.t_releves_occtax AS tro
                WHERE tro.unique_id_sinp_grp = roit.unique_id_sinp_grp
            )
            AND roit.meta_last_action = 'I'
        ORDER BY roit.gid ASC
        -- With NOT EXISTS don't use OFFSET because it's eliminate previously inserted rows.
        -- OFFSET offsetCnt
        LIMIT step ;

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted PARENT acquistion frameworks rows: %', affectedRows ;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;

\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
