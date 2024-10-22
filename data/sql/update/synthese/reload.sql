-- Re-insert all meshes (M1, M2, M5, M10, ...), departements (DEP), municipalities (COM),
-- regions (REG) and SINP area in gn_synthese.cor_area_synthese table for a specific import.
--
-- Required rights: DB OWNER
-- GeoNature database compatibility : v2.6.2+
-- Transfert this script on server this way:
-- rsync -av ./reload_cor_area_synthese.sql geonat@db-paca-sinp:~/data/shared/data/sql/ --dry-run
-- Use this script this way:
--      cat ~/data/shared/data/sql/update/synthse/reload.sql | \
--      sed 's/${syntheseImportTable}/<prefix>_synthese/g' | \
--      psql -h localhost -U geonatadmin -d geonature2db
-- Parameters:
--      syntheseImportTable: name of synthese import table to use. See gn_imports schema.
--                           Ex.: cbnmc_20221130_synthese.

\timing

BEGIN;


\echo '----------------------------------------------------------------------------'
\echo 'Create subdivided REG, DEP and COM areas table'

\echo ' Drop subdivided REG, DEP and COM areas table'
DROP TABLE IF EXISTS ref_geo.subdivided_areas ;

\echo ' Add subdivided REG, DEP and COM areas table'
-- SINP AURA Preprod: rows in
CREATE TABLE IF NOT EXISTS ref_geo.subdivided_areas AS
    SELECT
        random() AS gid,
        a.id_area AS area_id,
        bat.type_code AS code_type,
        a.area_code,
        st_subdivide(a.geom, 250) AS geom
    FROM ref_geo.l_areas AS a
        JOIN ref_geo.bib_areas_types AS bat
            ON bat.id_type = a.id_type
    WHERE a."enable" = TRUE
        AND bat.type_code IN ('REG', 'DEP', 'COM') ;

\echo ' Create index on geom column for subdivided REG, DEP and COM areas table'
CREATE INDEX IF NOT EXISTS idx_subdivided_geom
ON ref_geo.subdivided_areas USING gist(geom);

\echo ' Create index on column id_area for subdivided REG, DEP and COM areas table'
CREATE INDEX IF NOT EXISTS idx_subdivided_area_id
ON ref_geo.subdivided_areas USING btree(area_id) ;


\echo '----------------------------------------------------------------------------'
\echo 'Drop geom_synthese table'
DROP TABLE IF EXISTS gn_synthese.geom_synthese ;

\echo ' Create geom_synthese table with observations ids group by geom'
-- SINP AURA Preprod:  rows in
CREATE TABLE IF NOT EXISTS gn_synthese.geom_synthese AS (
    SELECT
        s.the_geom_local,
        array_agg(s.id_synthese) AS id_syntheses
    FROM gn_synthese.synthese AS s
        LEFT JOIN gn_imports.${syntheseImportTable} AS i
            ON i.unique_id_sinp = s.unique_id_sinp
    WHERE i.meta_last_action IN ('I', 'U')
    GROUP BY the_geom_local
) ;

\echo ' Create index on geom column for unique geom on synthese table'
-- SINP AURA Preprod:
CREATE INDEX IF NOT EXISTS idx_geom_synthese_geom
ON gn_synthese.geom_synthese USING gist(the_geom_local);


\echo '----------------------------------------------------------------------------'
\echo 'Drop flatten_meshes table'
DROP TABLE IF EXISTS ref_geo.flatten_meshes ;

\echo ' Create flatten_meshes table with meshes M1, M2, M5, M10, M20, M50'
-- SINP AURA Preprod:  rows in
CREATE TABLE IF NOT EXISTS ref_geo.flatten_meshes AS (
    SELECT
        m1.id_area AS id_m1,
        m2.id_area AS id_m2,
        m5.id_area AS id_m5,
        m10.id_area AS id_m10,
        m20.id_area AS id_m20,
        m50.id_area AS id_m50
    FROM (
            SELECT id_area, geom, centroid
            FROM ref_geo.l_areas
            WHERE id_type = ref_geo.get_id_area_type('M1')
        ) AS m1
        LEFT JOIN (
            SELECT id_area, geom
            FROM ref_geo.l_areas
            WHERE id_type = ref_geo.get_id_area_type('M2')
        ) AS m2
            ON st_contains(m2.geom, m1.centroid)
        LEFT JOIN (
            SELECT id_area, geom
            FROM ref_geo.l_areas
            WHERE id_type = ref_geo.get_id_area_type('M5')
        ) AS m5
            ON st_contains(m5.geom, m1.centroid)
        LEFT JOIN (
            SELECT id_area, geom
            FROM ref_geo.l_areas
            WHERE id_type = ref_geo.get_id_area_type('M10')
        ) AS m10
            ON  st_contains(m10.geom, m1.centroid)
        LEFT JOIN (
            SELECT id_area, geom
            FROM ref_geo.l_areas
            WHERE id_type = ref_geo.get_id_area_type('M20')
        ) AS m20
            ON st_contains(m20.geom, m1.centroid)
        LEFT JOIN (
            SELECT id_area, geom
            FROM ref_geo.l_areas
            WHERE id_type = ref_geo.get_id_area_type('M50')
        ) AS m50
            ON st_contains(m50.geom, m1.centroid)
) ;

\echo ' Create index on column id_m1 for flatten_meshes table'
CREATE INDEX IF NOT EXISTS id_m1_flatten_meshes_idx
ON ref_geo.flatten_meshes USING btree(id_m1);


\echo '----------------------------------------------------------------------------'
\echo 'Drop synthese_geom_dep table'
DROP TABLE IF EXISTS gn_synthese.synthese_geom_dep ;

\echo 'Create synthese_geom_dep table'
CREATE TABLE IF NOT EXISTS gn_synthese.synthese_geom_dep AS (
    SELECT DISTINCT
        s.the_geom_local AS geom,
        s.id_syntheses,
        a.area_id,
        a.area_code
    FROM gn_synthese.geom_synthese AS s
        INNER JOIN ref_geo.subdivided_areas AS a
            ON ( a.code_type = 'DEP' AND st_intersects(s.the_geom_local, a.geom) )
) ;

\echo ' Create index on geom column for synthese_geom_dep table'
CREATE INDEX IF NOT EXISTS idx_synthese_geom_dep_geom
ON gn_synthese.synthese_geom_dep USING gist(geom);

\echo ' Create index on column area_code for synthese_geom_dep table'
CREATE INDEX IF NOT EXISTS idx_synthese_geom_dep_area_code
ON gn_synthese.synthese_geom_dep USING btree(area_code) ;


\echo '----------------------------------------------------------------------------'
\echo 'Drop area_syntheses table'
DROP TABLE IF EXISTS gn_synthese.area_syntheses ;

\echo 'Create area_syntheses table'
CREATE TABLE IF NOT EXISTS gn_synthese.area_syntheses AS (
    SELECT DISTINCT
        s.id_syntheses,
        a.area_id
    FROM gn_synthese.synthese_geom_dep AS s
        LEFT JOIN ref_geo.subdivided_areas AS a
            ON ( a.code_type = 'COM' AND LEFT(a.area_code, 2) = s.area_code )
    WHERE st_intersects(s.geom, a.geom)

    UNION ALL

    SELECT
        id_syntheses,
        area_id
    FROM gn_synthese.synthese_geom_dep

    UNION ALL

    SELECT DISTINCT
        s.id_syntheses,
        a.id_area AS area_id
    FROM gn_synthese.synthese_geom_dep AS s
        LEFT JOIN (
            SELECT id_area
            FROM ref_geo.l_areas
            WHERE id_type = ref_geo.get_id_area_type('REG')
        ) AS a ON TRUE
) ;

\echo ' Create index on column area_id for area_syntheses table'
CREATE INDEX IF NOT EXISTS idx_area_syntheses_area_id
ON gn_synthese.area_syntheses USING btree(area_id) ;


\echo '----------------------------------------------------------------------------'
\echo 'Drop synthese_geom_m1 table'
DROP TABLE IF EXISTS gn_synthese.synthese_geom_m1 ;

\echo 'Create synthese_geom_m1 temporary table'
-- SINP AURA Preprod:  rows in
CREATE TABLE IF NOT EXISTS gn_synthese.synthese_geom_m1 AS (
    SELECT DISTINCT
        s.id_syntheses,
        a.id_area AS id_m1
    FROM gn_synthese.geom_synthese AS s
        INNER JOIN ref_geo.l_areas AS a
            ON (
                a.id_type = ref_geo.get_id_area_type('M1')
                AND st_intersects(s.the_geom_local, a.geom)
            )
) ;

\echo ' Create index on column id_m1 for synthese_geom_m1 table'
-- 6s
CREATE INDEX IF NOT EXISTS idx_synthese_geom_m1_id_m1
ON gn_synthese.synthese_geom_m1 USING btree(id_m1) ;


\echo '----------------------------------------------------------------------------'
\echo 'Drop synthese_geom_meshes table'
DROP TABLE IF EXISTS gn_synthese.synthese_geom_meshes ;

\echo 'Create synthese_geom_meshes temporary table'
-- SINP AURA Preprod:  rows in
CREATE TABLE IF NOT EXISTS gn_synthese.synthese_geom_meshes AS (
    SELECT
        id_syntheses,
        id_m1 AS id_mesh
    FROM gn_synthese.synthese_geom_m1

    UNION

    SELECT
        sgm.id_syntheses,
        fm.id_m2 AS id_mesh
    FROM gn_synthese.synthese_geom_m1 AS sgm
        LEFT JOIN ref_geo.flatten_meshes AS fm
            ON sgm.id_m1 = fm.id_m1

    UNION

    SELECT
        sgm.id_syntheses,
        fm.id_m5 AS id_mesh
    FROM gn_synthese.synthese_geom_m1 AS sgm
        LEFT JOIN ref_geo.flatten_meshes AS fm
            ON sgm.id_m1 = fm.id_m1

    UNION

    SELECT
        sgm.id_syntheses,
        fm.id_m10 AS id_mesh
    FROM gn_synthese.synthese_geom_m1 AS sgm
        LEFT JOIN ref_geo.flatten_meshes AS fm
            ON sgm.id_m1 = fm.id_m1

    UNION

    SELECT
        sgm.id_syntheses,
        fm.id_m20 AS id_mesh
    FROM gn_synthese.synthese_geom_m1 AS sgm
        LEFT JOIN ref_geo.flatten_meshes AS fm
            ON sgm.id_m1 = fm.id_m1

    UNION

    SELECT
        sgm.id_syntheses,
        fm.id_m50 AS id_mesh
    FROM gn_synthese.synthese_geom_m1 AS sgm
        LEFT JOIN ref_geo.flatten_meshes AS fm
            ON sgm.id_m1 = fm.id_m1
) ;

\echo ' Create index on column id_mesh for synthese_geom_meshes table'
-- 36s
CREATE INDEX IF NOT EXISTS idx_synthese_geom_meshes_id_mesh
ON gn_synthese.synthese_geom_meshes USING btree(id_mesh) ;


\echo '----------------------------------------------------------------------------'
\echo 'Drop synthese_sinp table'
DROP TABLE IF EXISTS gn_synthese.synthese_sinp ;

\echo 'Create synthese_sinp temporary table'
-- SINP AURA Preprod:  rows in
CREATE TABLE IF NOT EXISTS gn_synthese.synthese_sinp AS (
    WITH sinp AS (
        SELECT id_area
        FROM ref_geo.l_areas
        WHERE id_type = ref_geo.get_id_area_type('SINP')
        LIMIT 1
    )
    SELECT
        UNNEST(gs.id_syntheses) AS id_synthese,
        sinp.id_area
    FROM gn_synthese.geom_synthese AS gs, sinp
) ;

\echo ' Create index on column id_synthese for synthese_sinp table'
CREATE INDEX IF NOT EXISTS idx_synthese_sinp_id_synthese
ON gn_synthese.synthese_sinp USING btree(id_synthese) ;


-- \echo '----------------------------------------------------------------------------'
-- \echo 'Delete cor_area_synthese indexes and constraints'
-- Don't drop id_area index because it's used by delete queries
-- DROP INDEX IF EXISTS gn_synthese.cor_area_synthese_id_area_idx ;

-- DROP INDEX IF EXISTS gn_synthese.cor_area_synthese_id_synthese_idx ;

-- ALTER TABLE gn_synthese.cor_area_synthese
-- DROP CONSTRAINT IF EXISTS fk_cor_area_synthese_id_area ;

-- ALTER TABLE gn_synthese.cor_area_synthese
-- DROP CONSTRAINT IF EXISTS fk_cor_area_synthese_id_synthese ;

-- ALTER TABLE gn_synthese.cor_area_synthese
-- DROP CONSTRAINT IF EXISTS pk_cor_area_synthese ;


\echo '----------------------------------------------------------------------------'
\echo 'Clean all data imported into cor_area_synthese table'
-- SINP AURA Preprod:  rows in
-- DELETE FROM gn_synthese.cor_area_synthese AS c
-- USING gn_synthese.synthese AS s, gn_imports.:syntheseImportTable AS i
-- WHERE c.id_synthese = s.id_synthese
--     AND s.unique_id_sinp = i.unique_id_sinp ;

DO $$
DECLARE
    step INTEGER;
    stopAt INTEGER;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    SELECT COUNT(*) INTO stopAt FROM gn_imports.${syntheseImportTable} ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to delete in "cor_area_synthese" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to delete % observations from %', step, offsetCnt ;

        DELETE FROM gn_synthese.cor_area_synthese
        WHERE id_synthese = ANY(ARRAY(
            SELECT s.id_synthese
            FROM gn_imports.${syntheseImportTable} AS i
                JOIN gn_synthese.synthese AS s
                    ON s.unique_id_sinp = i.unique_id_sinp
            ORDER BY i.gid ASC
            OFFSET offsetCnt
            LIMIT step
        )) ;

        GET DIAGNOSTICS affectedRows = ROW_COUNT;

        RAISE NOTICE 'Deleted cor_area_synthese rows: %', affectedRows ;

        offsetCnt := offsetCnt + (step) ;

        -- Optionally, include a delay to reduce load on the server
        PERFORM pg_sleep(1);
    END LOOP;
END $$;


\echo '----------------------------------------------------------------------------'
\echo 'Reinsert Régions, Départements and Communes into cor_area_synthese'

-- SINP AURA Preprod:  rows in
DO $$
DECLARE
    step INTEGER;
    stopAt INTEGER;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    -- Set dynamicly stopAt and step
    SELECT COUNT(*) INTO stopAt FROM gn_synthese.area_syntheses ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to insert in "synthese" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to insert % observations from %', step, offsetCnt ;

        WITH admin_zones AS (
            SELECT
                id_syntheses,
                area_id
            FROM gn_synthese.area_syntheses AS a
            ORDER BY a.area_id ASC
            OFFSET offsetCnt
            LIMIT step
        )
        INSERT INTO gn_synthese.cor_area_synthese (
            id_synthese,
            id_area
        )
            SELECT
                UNNEST(id_syntheses) AS id_synthese,
                area_id
            FROM admin_zones ;

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted cor_area_synthese rows: %', affectedRows ;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;


\echo '----------------------------------------------------------------------------'
\echo 'Reinsert all meshes into cor_area_synthese'

-- SINP AURA Preprod:  rows in
DO $$
DECLARE
    step INTEGER;
    stopAt INTEGER;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    -- Set dynamicly stopAt and step
    SELECT COUNT(*) INTO stopAt FROM gn_synthese.synthese_geom_meshes ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to insert in "synthese" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to insert % observations from %', step, offsetCnt ;

        WITH meshes AS (
            SELECT
                id_syntheses,
                id_mesh
            FROM gn_synthese.synthese_geom_meshes AS sgm
            ORDER BY sgm.id_mesh ASC
            OFFSET offsetCnt
            LIMIT step
        )
        INSERT INTO gn_synthese.cor_area_synthese (
            id_synthese,
            id_area
        )
            SELECT
                UNNEST(id_syntheses) AS id_synthese,
                id_mesh
            FROM meshes ;

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted cor_area_synthese rows: %', affectedRows ;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;


\echo '----------------------------------------------------------------------------'
\echo 'Reinsert all observations in cor_area_synthese link to SINP area'

-- SINP AURA Preprod:  rows in
DO $$
DECLARE
    step INTEGER;
    stopAt INTEGER;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    -- Set dynamicly stopAt and step
    SELECT COUNT(*) INTO stopAt FROM gn_synthese.synthese_sinp ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to insert in "synthese" table' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to insert % observations from %', step, offsetCnt ;

        INSERT INTO gn_synthese.cor_area_synthese (
            id_synthese,
            id_area
        )
            SELECT
                id_synthese,
                id_area
            FROM gn_synthese.synthese_sinp AS ss
            ORDER BY ss.id_synthese ASC
            OFFSET offsetCnt
            LIMIT step ;

        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted cor_area_synthese rows: %', affectedRows ;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;

-- \echo '----------------------------------------------------------------------------'
-- \echo 'Recreate cor_area_synthese indexes and constraints'
-- 8mn 33s
-- ALTER TABLE gn_synthese.cor_area_synthese
-- ADD CONSTRAINT pk_cor_area_synthese PRIMARY KEY (id_synthese, id_area) ;

-- 3mn 28s
-- ALTER TABLE gn_synthese.cor_area_synthese
-- ADD CONSTRAINT fk_cor_area_synthese_id_area
-- FOREIGN KEY (id_area) REFERENCES ref_geo.l_areas(id_area)
-- ON DELETE CASCADE ON UPDATE CASCADE ;

-- 7mn 36s
-- ALTER TABLE gn_synthese.cor_area_synthese
-- ADD CONSTRAINT fk_cor_area_synthese_id_synthese
-- FOREIGN KEY (id_synthese) REFERENCES gn_synthese.synthese(id_synthese)
-- ON DELETE CASCADE ON UPDATE CASCADE ;

-- The id_area index was not deleted because delete queries uses it.
-- 7mn
-- CREATE INDEX cor_area_synthese_id_area_idx
-- ON gn_synthese.cor_area_synthese USING btree(id_area);

-- 7mn 17s
-- CREATE INDEX cor_area_synthese_id_synthese_idx
-- ON gn_synthese.cor_area_synthese USING btree(id_synthese);


\echo '----------------------------------------------------------------------------'
\echo 'Clean all temporary tables'

\echo ' Drop subdivided REG, DEP and COM areas table'
DROP TABLE IF EXISTS ref_geo.subdivided_areas ;

\echo ' Drop geom_synthese table'
DROP TABLE IF EXISTS gn_synthese.geom_synthese ;

\echo ' Drop flatten_meshes table'
DROP TABLE IF EXISTS ref_geo.flatten_meshes ;

\echo ' Drop synthese_geom_dep table'
DROP TABLE IF EXISTS gn_synthese.synthese_geom_dep ;

\echo ' Drop area_syntheses table'
DROP TABLE IF EXISTS gn_synthese.area_syntheses ;

\echo ' Drop synthese_geom_m1 table'
DROP TABLE IF EXISTS gn_synthese.synthese_geom_m1 ;

\echo ' Drop synthese_geom_meshes table'
DROP TABLE IF EXISTS gn_synthese.synthese_geom_meshes ;

\echo ' Drop synthese_sinp table'
DROP TABLE IF EXISTS gn_synthese.synthese_sinp ;


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is OK:'
COMMIT;
