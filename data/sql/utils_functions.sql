\echo 'Insert utils functions'
\echo 'Required rights: db owner'
\echo 'GeoNature database compatibility : v2.3.0+'
BEGIN;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "get_id_area_type_by_code()"'
CREATE OR REPLACE FUNCTION ref_geo.get_id_area_type_by_code(areaTypeCode character varying)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_type from a bib_areas_types
    DECLARE idType integer;

    BEGIN
        SELECT INTO idType id_type
        FROM ref_geo.bib_areas_types AS bat
        WHERE bat.type_code = areaTypeCode ;

        RETURN idType ;
    END;
$function$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "get_id_acquisition_framework_by_name()"'
CREATE OR REPLACE FUNCTION gn_meta.get_id_acquisition_framework_by_name(afName character varying)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_acquisition_framework from an acquition_framework_name
    DECLARE idAcquisitionFramework integer;

    BEGIN
        SELECT INTO idAcquisitionFramework id_acquisition_framework
        FROM gn_meta.t_acquisition_frameworks AS af
        WHERE af.acquisition_framework_name = afName ;

        RETURN idAcquisitionFramework ;
    END;
$function$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "get_id_acquisition_framework_by_uuid()"'
CREATE OR REPLACE FUNCTION gn_meta.get_id_acquisition_framework_by_uuid(afUuid uuid)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_acquisition_framework from an unique_acquisition_framework_id
    DECLARE idAcquisitionFramework integer;

    BEGIN
        SELECT INTO idAcquisitionFramework id_acquisition_framework
        FROM gn_meta.t_acquisition_frameworks AS af
        WHERE af.unique_acquisition_framework_id = afUuid ;

        RETURN idAcquisitionFramework ;
    END;
$function$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "get_id_dataset_by_shortname()"'
CREATE OR REPLACE FUNCTION gn_meta.get_id_dataset_by_shortname(shortName character varying)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_dataset from an dataset_shortname
    DECLARE idDataset integer;

    BEGIN
        SELECT INTO idDataset id_dataset
        FROM gn_meta.t_datasets AS td
        WHERE td.dataset_shortname = shortName ;

        RETURN idDataset ;
    END;
$function$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "get_id_dataset_by_uuid()"'
CREATE OR REPLACE FUNCTION gn_meta.get_id_dataset_by_uuid(dUuid uuid)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_dataset from an unique_dataset_id
    DECLARE idDataset integer;

    BEGIN
        SELECT INTO idDataset id_dataset
        FROM gn_meta.t_datasets AS td
        WHERE td.unique_dataset_id = dUuid ;

        RETURN idDataset ;
    END;
$function$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "get_id_publication_by_reference()"'
CREATE OR REPLACE FUNCTION gn_meta.get_id_publication_by_reference(reference VARCHAR)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_publication from a reference
    DECLARE idPublication integer;

    BEGIN
        SELECT INTO idPublication id_publication
        FROM gn_meta.sinp_datatype_publications AS sdp
        WHERE sdp.publication_reference = reference ;

        RETURN idPublication ;
    END;
$function$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "get_id_organism_by_name()"'
CREATE OR REPLACE FUNCTION utilisateurs.get_id_organism_by_name(oName character varying)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_organisme from an nom_organisme
    DECLARE idOrganism integer;

    BEGIN
        SELECT INTO idOrganism id_organisme
        FROM utilisateurs.bib_organismes AS bo
        WHERE bo.nom_organisme = oName ;

        RETURN idOrganism ;
    END;
$function$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "get_id_organism_by_uuid()"'
CREATE OR REPLACE FUNCTION utilisateurs.get_id_organism_by_uuid(oUuid uuid)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_organisme from an uuid_organisme
    DECLARE idOrganism integer;

    BEGIN
        SELECT INTO idOrganism id_organisme
        FROM utilisateurs.bib_organismes AS bo
        WHERE bo.uuid_organisme = oUuid ;

        RETURN idOrganism ;
    END;
$function$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "get_id_role_by_identifier()"'
CREATE OR REPLACE FUNCTION utilisateurs.get_id_role_by_identifier(identifier character varying)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_role from an identifer
    DECLARE idRole integer;

    BEGIN
        SELECT INTO idRole id_role
        FROM utilisateurs.t_roles AS tr
        WHERE tr.identifiant = identifier ;

        RETURN idRole ;
    END;
$function$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "get_id_role_by_uuid()"'
CREATE OR REPLACE FUNCTION utilisateurs.get_id_role_by_uuid(rUuid uuid)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_role from an uuid_role
    DECLARE idRole integer;

    BEGIN
        SELECT INTO idRole id_role
        FROM utilisateurs.t_roles AS tr
        WHERE tr.uuid_role = rUuid ;

        RETURN idRole ;
    END;
$function$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function"get_id_group_by_name()"'
CREATE OR REPLACE FUNCTION utilisateurs.get_id_group_by_name(groupName character varying)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
AS $function$
    -- Function which return the id_role of a group by its name
    DECLARE idRole integer;

    BEGIN
        SELECT INTO idRole tr.id_role
        FROM utilisateurs.t_roles AS tr
        WHERE tr.nom_role = groupName
            AND tr.groupe = true ;

        RETURN idRole ;
    END;
$function$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "get_id_source_by_name()"'
CREATE OR REPLACE FUNCTION gn_synthese.get_id_source_by_name(sourceName character varying)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_source from an name_source
    DECLARE idSource integer;

    BEGIN
        SELECT INTO idSource id_source
        FROM gn_synthese.t_sources AS ts
        WHERE ts.name_source = sourceName ;

        RETURN idSource ;
    END;
$function$ ;


\echo '----------------------------------------------------------------------------'
\echo 'Set function "get_id_module_by_code()"'
CREATE OR REPLACE FUNCTION gn_commons.get_id_module_by_code(moduleCode character varying)
    RETURNS int
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_module from a module code
    DECLARE idModule INTEGER;

    BEGIN
        SELECT id_module INTO idModule
        FROM gn_commons.t_modules
        WHERE module_code ILIKE moduleCode ;

        RETURN idModule ;
    END ;
$function$ ;


\echo '----------------------------------------------------------------------------'
\echo 'Set function "get_id_action_by_code()"'
CREATE OR REPLACE FUNCTION gn_permissions.get_id_action_by_code(actionCode character varying)
    RETURNS int
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_action from an action code
    DECLARE idAction INTEGER;

    BEGIN
        SELECT id_action INTO idAction
        FROM gn_permissions.bib_actions
        WHERE code_action ILIKE actionCode ;

        RETURN idAction ;
    END ;
$function$ ;


\echo '----------------------------------------------------------------------------'
\echo 'Set function "get_id_object_by_code()"'
CREATE OR REPLACE FUNCTION gn_permissions.get_id_object_by_code(objectCode character varying)
    RETURNS int
    LANGUAGE plpgsql
    IMMUTABLE
AS
$function$
    -- Function which return the id_object from an object code
    DECLARE idObject INTEGER;

    BEGIN
        SELECT id_object INTO idObject
        FROM gn_permissions.t_objects
        WHERE code_object ILIKE objectCode ;

        RETURN idObject ;
    END ;
$function$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "reset_sequence()"'
CREATE OR REPLACE FUNCTION public.reset_sequence(table_schema text, tablename text, columnname text, sequence_name text)
    RETURNS "pg_catalog"."void"
    LANGUAGE plpgsql
AS
$function$
      DECLARE
      BEGIN
        EXECUTE 'LOCK TABLE ' || table_schema || '.'|| tablename || ' IN EXCLUSIVE MODE ;' ;
        EXECUTE 'SELECT setval( ''' || sequence_name  || ''', ' || '(SELECT MAX(' || columnname ||
            ') FROM ' || table_schema || '.'|| tablename || ')' || '+1)' ;
      END ;
$function$ ;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "computeImportTotal()"'
CREATE OR REPLACE FUNCTION gn_imports.computeImportTotal(
    tableImport VARCHAR, actionImport VARCHAR, OUT total integer
)
LANGUAGE plpgsql AS $function$
    DECLARE
        schemaImport VARCHAR ;
        parsed_ident VARCHAR[] ;
        parsed_count INT ;
    BEGIN
        parsed_ident := parse_ident(tableImport) ;
        parsed_count := array_length(parsed_ident, 1) ;

        IF parsed_count = 2 THEN
            SELECT parsed_ident[1] INTO schemaImport ;
            SELECT parsed_ident[2] INTO tableImport ;
        ELSIF parsed_count = 1 THEN
            schemaImport := 'gn_imports' ;
            SELECT parsed_ident[1] INTO tableImport ;
        END IF;

        --RAISE NOTICE 'Schema %, table %', schemaImport, tableImport ;
        EXECUTE format(
            'SELECT COUNT(*) FROM %I.%I WHERE meta_last_action = $1 ',
            schemaImport, tableImport
        ) USING actionImport INTO total ;
    END;
$function$;


\echo '-------------------------------------------------------------------------------'
\echo 'Set function "computeImportStep()"'
CREATE OR REPLACE FUNCTION gn_imports.computeImportStep(total INT)
RETURNS INT
LANGUAGE plpgsql
AS
$function$
    BEGIN
        IF total <= 100000 THEN
            RETURN 10000;
        ELSIF total > 100000 AND total <= 2500000 THEN
            RETURN 100000;
        ELSIF total > 2500000 THEN
            RETURN 500000;
        END IF;
    END;
$function$;

\echo '-------------------------------------------------------------------------------'
\echo 'Set function "clean_observers_uuid()"'
CREATE OR REPLACE FUNCTION gn_imports.clean_observers_uuid(observersImported varchar)
    RETURNS text
    LANGUAGE plpgsql
    SECURITY DEFINER
AS
$function$
    --
    DECLARE
        observersExported text;

    BEGIN
        observersExported := '';

        SELECT INTO observersExported
            regexp_replace(observersImported,'[\s]\[(.*?)\]','','g');

        RETURN observersExported;
    END;
$function$ ;

\echo '-------------------------------------------------------------------------------'
\echo 'Set function "public.fct_trg_meta_dates_change()"'
CREATE OR REPLACE FUNCTION public.fct_trg_meta_dates_change()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
-- Modify function to keep origin date when exists.
BEGIN
        IF(TG_OP = 'INSERT') THEN
                -- Test if NEW.meta_create_date is null or empty
                if (NEW.meta_create_date IS NULL) THEN
                        NEW.meta_create_date = NOW();
                END IF;
        ELSIF(TG_OP = 'UPDATE') THEN
                -- Test if NEW.meta_create_date is null or empty
                if (NEW.meta_create_date IS NULL) THEN
                    NEW.meta_update_date = NOW();
                END IF;

                -- Test if NEW.meta_update_date is null or empty
                if (NEW.meta_update_date IS NULL) THEN
                        NEW.meta_create_date = NOW();
                END IF;
        END IF;
        return NEW;
END;
$function$
;

\echo '-------------------------------------------------------------------------------'
\echo 'Set function "utilisateurs.modify_date_insert()"'
-- Modify function to keep origin date when exists.
CREATE OR REPLACE FUNCTION utilisateurs.modify_date_insert()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
-- Modify function to keep origin date when exists.
BEGIN
    -- Test if NEW.date_insert is null or empty
    IF (NEW.date_insert IS NULL) THEN
        NEW.date_insert := NOW();
    END IF;
    -- Test if NEW.date_update is null or empty
    IF (NEW.date_update IS NULL) THEN
        NEW.date_update := NOW();
    END IF;
    RETURN NEW;
END;
$function$
;

\echo '-------------------------------------------------------------------------------'
\echo 'Set function "utilisateurs.modify_date_update()"'
CREATE OR REPLACE FUNCTION utilisateurs.modify_date_update()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
-- Modify function to keep origin date when exists.
BEGIN
    -- Test if NEW.date_update is null or empty
    IF (NEW.date_update IS NULL) THEN
        NEW.date_update := NOW();
    END IF;
    RETURN NEW;
END;
$function$
;


\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
