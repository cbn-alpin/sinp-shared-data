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
\echo 'Set function "extract_observers_uuid()"'
CREATE OR REPLACE FUNCTION gn_imports.extract_observers_uuid(varchar)
    RETURNS uuid[]
    LANGUAGE plpgsql
    RETURNS NULL ON NULL INPUT
AS
$function$
    -- Extract the UUID enclosed in square brackets for each  comma-separated observer string.
    DECLARE
        observersStr ALIAS FOR $1;
        observers varchar[];
        observer varchar;
        extracted_uuid varchar;
        observersUuids uuid[];

    BEGIN
        observers = string_to_array(observersStr, '[');
        FOR idx IN array_lower(observers, 1)..array_upper(observers, 1) LOOP
            observer = observers[idx];
            -- Avoid to use string was not an UUID (UUID = 36 characters)
            IF POSITION(']' IN observer) = 37 THEN
                extracted_uuid = substring(observer, 1, 36);
                observersUuids[idx] := CAST(extracted_uuid AS uuid);
            END IF;
        END LOOP;
        RETURN observersUuids;
    END;
$function$ ;

\echo '----------------------------------------------------------------------------'
\echo 'COMMIT if all is ok:'
COMMIT;
