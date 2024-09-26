BEGIN;
-- This file contain a variable "${occtaxImportTable}" which must be replaced
-- with "sed" before passing the updated content to psql.

\echo '-------------------------------------------------------------------------------'
\echo 'Insert imported occtax with meta_last_action = I.'
\echo 'Rights: db-owner'
\echo 'GeoNature database compatibility : v2.4.1+'

SET client_encoding = 'UTF8';

\echo '----------------------------------------------------------------------------'
\echo 'Verify if "pr_occtax" schema exists'
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.schemata
        WHERE schema_name = 'pr_occtax'
    ) THEN
        RAISE EXCEPTION 'Schema "pr_occtax" not found.'
            USING HINT = 'Please install Occtax module.';
    END IF;
END
$$;

\echo '-------------------------------------------------------------------------------'
\echo 'Disable trigger "tri_insert_synthese_cor_counting_occtax"'
-- trigger which runs "pr_occtax.fct_tri_synthese_insert_counting" which insert data in synthese
ALTER TABLE pr_occtax.cor_counting_occtax DISABLE TRIGGER tri_insert_synthese_cor_counting_occtax ;


\echo '-------------------------------------------------------------------------------'
\echo 'Batch updating in "t_releves_occtax, t_occurrences_occtax, cor_counting_occtax" of the imported occtax'
DO $$
DECLARE
    step INTEGER ;
    stopAt INTEGER ;
    offsetCnt INTEGER := 0 ;
    affectedRows INTEGER;
BEGIN
    -- Set dynamicly stopAt and step
    stopAt := gn_imports.computeImportTotal('gn_imports.${occtaxImportTable}', 'I') ;
    step := gn_imports.computeImportStep(stopAt) ;
    RAISE NOTICE 'Total found: %, step used: %', stopAt, step ;

    RAISE NOTICE 'Start to loop on data to insert in "t_releves_occtax, t_occurrences_occtax, cor_counting_occtax" tables' ;
    WHILE offsetCnt < stopAt LOOP

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Try to insert % occtax from %', step, offsetCnt ;

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Inserting occtax data to "t_releves_occtax" if not exist' ;
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
            id_module
        )
        SELECT
            unique_id_sinp_grp,
            code_dataset,
            code_digitiser,
            observers,
            code_nomenclature_tech_collect_campanule,
            code_nomenclature_grp_typ,
            grp_method,
            date_min::DATE,
            date_max::DATE,
            date_min::TIME,
            date_max::TIME,
            code_nomenclature_tech_collect_campanule,
            code_nomenclature_grp_typ::int,
            grp_method,
            date_min,
            date_max,
            cd_hab,
            altitude_min,
            altitude_max,
            depth_min,
            depth_max,
            place_name,
            meta_device_entry,
            comment_context,
            geom,
            ST_Transform(geom, 4326),
            code_nomenclature_geo_object_nature,
            precision,
            additional_fields,
            gn_commons.get_id_module_by_code('OCCTAX')
        FROM gn_imports.${occtaxImportTable} AS ocit
        WHERE NOT EXISTS (
                SELECT 'X'
                FROM pr_occtax.t_releves_occtax AS tro
                WHERE tro.unique_id_sinp_grp = ocit.unique_id_sinp_grp
            )
            AND ocit.meta_last_action = 'I'
        ORDER BY ocit.gid ASC
        -- With NOT EXISTS don't use OFFSET because it's eliminate previously inserted rows.
        -- OFFSET offsetCnt
        LIMIT step ;
        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted releves occtax rows: %', affectedRows ;

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Inserting occurrences occtax data to "t_occurences_occtax" if not exist' ;
        INSERT INTO pr_occtax.t_occurrences_occtax (
            unique_id_occurence_occtax,
            id_releve_occtax,
            id_releve_occtax,
            id_nomenclature_obs_technique,
            id_nomenclature_bio_condition,
            id_nomenclature_bio_status,
            id_nomenclature_naturalness,
            id_nomenclature_exist_proof,
            id_nomenclature_diffusion_level,
            id_nomenclature_observation_status,
            id_nomenclature_blurring,
            id_nomenclature_source_status,
            id_nomenclature_behaviour,
            determiner,
            id_nomenclature_determination_method,
            cd_nom,
            nom_cite,
            meta_v_taxref,
            sample_number_proof,
            digital_proof,
            non_digital_proof,
            comment,
            additional_fields
        )
        SELECT
            unique_id_occurence_occtax,
            pr_occtax.get_id_survey_by_uuid(unique_id_sinp_grp),
            code_nomenclature_obs_technique,
            code_nomenclature_bio_condition,
            code_nomenclature_bio_status,
            code_nomenclature_naturalness,
            code_nomenclature_exist_proof,
            code_nomenclature_diffusion_level,
            code_nomenclature_observation_status,
            code_nomenclature_blurring,
            code_nomenclature_source_status,
            code_nomenclature_behaviour,
            determiner,
            code_nomenclature_determination_method,
            cd_nom,
            nom_cite,
            meta_v_taxref,
            sample_number_proof,
            digital_proof,
            non_digital_proof,
            comment_description
        FROM gn_imports.${occtaxImportTable} AS ocit
        WHERE NOT EXISTS (
                SELECT 'X'
                FROM pr_occtax.t_occurrences_occtax AS too
                WHERE too.unique_id_occurence_occtax = ocit.unique_id_occurence_occtax
            )
            AND ocit.meta_last_action = 'I'
        ORDER BY ocit.gid ASC
        -- With NOT EXISTS don't use OFFSET because it's eliminate previously inserted rows.
        -- OFFSET offsetCnt
        LIMIT step ;
        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted occurrences occtax rows: %', affectedRows ;

        RAISE NOTICE '-------------------------------------------------' ;
        RAISE NOTICE 'Inserting counting occtax data to "cor_counting_occtax" if not exist' ;
        INSERT INTO pr_occtax.cor_counting_occtax (
            unique_id_sinp_occtax,
            id_occurrence_occtax,
            id_nomenclature_life_stage,
            id_nomenclature_sex,
            id_nomenclature_obj_count,
            id_nomenclature_type_count,
            count_min,
            count_max,
            additional_fields
        )
        SELECT
            unique_id_sinp_occtax,
            pr_occtax.get_id_occurrence_by_uuid(unique_id_occurence_occtax),
            code_nomenclature_life_stage,
            code_nomenclature_sex,
            code_nomenclature_obj_count,
            code_nomenclature_type_count,
            count_min,
            count_max
        FROM gn_imports.${occtaxImportTable} AS ocit
        WHERE NOT EXISTS (
                SELECT 'X'
                FROM pr_occtax.cor_counting_occtax AS cco
                WHERE cco.unique_id_sinp_occtax = ocit.unique_id_sinp_occtax
            )
            AND ocit.meta_last_action = 'I'
        ORDER BY ocit.gid ASC
        -- With NOT EXISTS don't use OFFSET because it's eliminate previously inserted rows.
        -- OFFSET offsetCnt
        LIMIT step ;
        GET DIAGNOSTICS affectedRows = ROW_COUNT;
        RAISE NOTICE 'Inserted counting occtax rows: %', affectedRows ;

        offsetCnt := offsetCnt + (step) ;
    END LOOP ;
END
$$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Re-create function "pr_occtax.insert_in_synthese" '

CREATE OR REPLACE FUNCTION pr_occtax.insert_in_synthese(my_id_counting integer)
 RETURNS integer[]
 LANGUAGE plpgsql
AS $function$  DECLARE
            new_count RECORD;
            occurrence RECORD;
            releve RECORD;
            id_source integer;
            id_nomenclature_source_status integer;
            myobservers RECORD;
            id_role_loop integer;

            BEGIN
            --recupération du counting à partir de son ID
            SELECT INTO new_count * FROM pr_occtax.cor_counting_occtax WHERE id_counting_occtax = my_id_counting;

            -- Récupération de l'occurrence
            SELECT INTO occurrence * FROM pr_occtax.t_occurrences_occtax occ WHERE occ.id_occurrence_occtax = new_count.id_occurrence_occtax;

            -- Récupération du relevé
            SELECT INTO releve * FROM pr_occtax.t_releves_occtax rel WHERE occurrence.id_releve_occtax = rel.id_releve_occtax;

            -- Récupération de la source
            SELECT INTO id_source s.id_source FROM gn_synthese.t_sources s WHERE id_module = releve.id_module;

            -- Récupération du status_source depuis le JDD
            SELECT INTO id_nomenclature_source_status d.id_nomenclature_source_status FROM gn_meta.t_datasets d WHERE id_dataset = releve.id_dataset;

            --Récupération et formatage des observateurs
            SELECT INTO myobservers array_to_string(array_agg(rol.nom_role || ' ' || rol.prenom_role), ', ') AS observers_name,
            array_agg(rol.id_role) AS observers_id
            FROM pr_occtax.cor_role_releves_occtax cor
            JOIN utilisateurs.t_roles rol ON rol.id_role = cor.id_role
            WHERE cor.id_releve_occtax = releve.id_releve_occtax;

            -- insertion dans la synthese
            INSERT INTO gn_synthese.synthese (
            unique_id_sinp,
            unique_id_sinp_grp,
            id_source,
            entity_source_pk_value,
            id_dataset,
            id_module,
            id_nomenclature_geo_object_nature,
            id_nomenclature_grp_typ,
            grp_method,
            id_nomenclature_obs_technique,
            id_nomenclature_bio_status,
            id_nomenclature_bio_condition,
            id_nomenclature_naturalness,
            id_nomenclature_exist_proof,
            id_nomenclature_diffusion_level,
            id_nomenclature_life_stage,
            id_nomenclature_sex,
            id_nomenclature_obj_count,
            id_nomenclature_type_count,
            id_nomenclature_observation_status,
            id_nomenclature_blurring,
            id_nomenclature_source_status,
            id_nomenclature_info_geo_type,
            id_nomenclature_behaviour,
            count_min,
            count_max,
            cd_nom,
            cd_hab,
            nom_cite,
            meta_v_taxref,
            sample_number_proof,
            digital_proof,
            non_digital_proof,
            altitude_min,
            altitude_max,
            depth_min,
            depth_max,
            place_name,
            precision,
            the_geom_4326,
            the_geom_point,
            the_geom_local,
            date_min,
            date_max,
            observers,
            determiner,
            id_digitiser,
            id_nomenclature_determination_method,
            comment_context,
            comment_description,
            last_action,
            additional_data
            )
            VALUES(
                new_count.unique_id_sinp_occtax,
                releve.unique_id_sinp_grp,
                id_source,
                new_count.id_counting_occtax,
                releve.id_dataset,
                releve.id_module,
                releve.id_nomenclature_geo_object_nature,
                releve.id_nomenclature_grp_typ,
                releve.grp_method,
                occurrence.id_nomenclature_obs_technique,
                occurrence.id_nomenclature_bio_status,
                occurrence.id_nomenclature_bio_condition,
                occurrence.id_nomenclature_naturalness,
                occurrence.id_nomenclature_exist_proof,
                occurrence.id_nomenclature_diffusion_level,
                new_count.id_nomenclature_life_stage,
                new_count.id_nomenclature_sex,
                new_count.id_nomenclature_obj_count,
                new_count.id_nomenclature_type_count,
                occurrence.id_nomenclature_observation_status,
                occurrence.id_nomenclature_blurring,
                -- status_source récupéré depuis le JDD
                id_nomenclature_source_status,
                -- id_nomenclature_info_geo_type: type de rattachement = non saisissable: georeferencement
                ref_nomenclatures.get_id_nomenclature('TYP_INF_GEO', '1'),
                occurrence.id_nomenclature_behaviour,
                new_count.count_min,
                new_count.count_max,
                occurrence.cd_nom,
                releve.cd_hab,
                occurrence.nom_cite,
                occurrence.meta_v_taxref,
                occurrence.sample_number_proof,
                occurrence.digital_proof,
                occurrence.non_digital_proof,
                releve.altitude_min,
                releve.altitude_max,
                releve.depth_min,
                releve.depth_max,
                releve.place_name,
                releve.precision,
                releve.geom_4326,
                ST_CENTROID(releve.geom_4326),
                releve.geom_local,
                date_trunc('day',releve.date_min)+COALESCE(releve.hour_min,'00:00:00'::time),
                date_trunc('day',releve.date_max)+COALESCE(releve.hour_max,'00:00:00'::time),
                COALESCE (myobservers.observers_name, releve.observers_txt),
                occurrence.determiner,
                releve.id_digitiser,
                occurrence.id_nomenclature_determination_method,
                releve.comment,
                occurrence.comment,
                'I',
                COALESCE(releve.additional_fields, '{}'::jsonb) || COALESCE(occurrence.additional_fields, '{}'::jsonb) || COALESCE(new_count.additional_fields, '{}'::jsonb)
            );

                RETURN myobservers.observers_id ;
            END;
            $function$
;

\echo '-------------------------------------------------------------------------------'
\echo 'Enable trigger "tri_insert_cor_area_synthese"'
ALTER TABLE gn_synthese.synthese ENABLE TRIGGER tri_insert_cor_area_synthese ;


\echo '-------------------------------------------------------------------------------'
\echo 'Enable trigger "tri_meta_dates_change_synthese"'
ALTER TABLE gn_synthese.synthese ENABLE TRIGGER tri_meta_dates_change_synthese ;


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
