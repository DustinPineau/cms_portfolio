CREATE OR REPLACE PROCEDURE adm.update_dim_provider()
LANGUAGE plpgsql
AS $$
DECLARE
    v_today DATE := CURRENT_DATE;
BEGIN

    -- close out existing records for any npi present in raw.nppes_bulk
    WITH latest_nppes AS (
        SELECT DISTINCT ON (data->>'NPI')
            data->>'NPI' AS npi
        FROM raw.nppes_bulk
        ORDER BY data->>'NPI', loaded_at DESC
    )
    UPDATE analytics.dim_provider
    SET
        valid_to = v_today,
        is_current = false
    WHERE npi IN (SELECT npi FROM latest_nppes)
    AND is_current = true;

    -- insert new versions for all npis in raw.nppes_bulk
    WITH latest_nppes AS (
        SELECT DISTINCT ON (data->>'NPI')
            data->>'NPI'                                                                    AS npi,
            data->>'Entity Type Code'                                                       AS entity_type_code,
            data->>'Provider Last Name (Legal Name)'                                        AS last_name,
            data->>'Provider First Name'                                                    AS first_name,
            data->>'Provider Business Practice Location Address City Name'                  AS location_city,
            data->>'Provider Business Practice Location Address State Name'                 AS location_state,
            data->>'Provider Business Practice Location Address Postal Code'                AS location_postal_code,
            data->>'Provider Business Practice Location Address Telephone Number'           AS location_telephone,
            data->>'Provider First Line Business Practice Location Address'                 AS location_address_1,
            data->>'Provider Second Line Business Practice Location Address'                AS location_address_2,
            data->>'Provider First Line Business Mailing Address'                           AS mailing_address_1,
            data->>'Provider Second Line Business Mailing Address'                          AS mailing_address_2,
            data->>'Provider Business Mailing Address City Name'                            AS mailing_city,
            data->>'Provider Business Mailing Address State Name'                           AS mailing_state,
            data->>'Provider Business Mailing Address Postal Code'                          AS mailing_postal_code,
            data->>'Provider Business Mailing Address Telephone Number'                     AS mailing_telephone,
            data->>'Provider Credential Text'                                               AS credential,
            data->>'Provider Sex Code'                                                      AS sex,
            data->>'Is Sole Proprietor'                                                     AS sole_proprietor,
            data->>'Provider Enumeration Date'                                              AS enumeration_date,
            data->>'Last Update Date'                                                       AS last_updated,
            data->>'NPI Deactivation Date'                                                  AS deactivation_date,
            data->>'NPI Reactivation Date'                                                  AS reactivation_date,
            data->>'Healthcare Provider Taxonomy Code_1'                                    AS taxonomy_code_1,
            data->>'Provider License Number_1'                                              AS taxonomy_license_1,
            data->>'Provider License Number State Code_1'                                   AS taxonomy_state_1,
            data->>'Healthcare Provider Taxonomy Code_2'                                    AS taxonomy_code_2,
            data->>'Provider License Number_2'                                              AS taxonomy_license_2,
            data->>'Provider License Number State Code_2'                                   AS taxonomy_state_2,
            data->>'Healthcare Provider Taxonomy Code_3'                                    AS taxonomy_code_3,
            data->>'Provider License Number_3'                                              AS taxonomy_license_3,
            data->>'Provider License Number State Code_3'                                   AS taxonomy_state_3,
            data->>'Healthcare Provider Taxonomy Code_4'                                    AS taxonomy_code_4,
            data->>'Provider License Number_4'                                              AS taxonomy_license_4,
            data->>'Provider License Number State Code_4'                                   AS taxonomy_state_4,
            data->>'Healthcare Provider Taxonomy Code_5'                                    AS taxonomy_code_5,
            data->>'Provider License Number_5'                                              AS taxonomy_license_5,
            data->>'Provider License Number State Code_5'                                   AS taxonomy_state_5,
            loaded_at
        FROM raw.nppes_bulk
        ORDER BY data->>'NPI', loaded_at DESC
    )
    INSERT INTO analytics.dim_provider (
        provider_key,
        npi,
        prscrbr_last_org_name,
        prscrbr_first_name,
        entity_type_code,
        credential,
        sex,
        sole_proprietor,
        enumeration_date,
        last_updated,
        deactivation_date,
        reactivation_date,
        mailing_address_1,
        mailing_address_2,
        mailing_city,
        mailing_state,
        mailing_postal_code,
        mailing_telephone,
        location_address_1,
        location_address_2,
        location_city,
        location_state,
        location_postal_code,
        location_telephone,
        taxonomy_code_1,
        taxonomy_license_1,
        taxonomy_state_1,
        taxonomy_code_2,
        taxonomy_license_2,
        taxonomy_state_2,
        taxonomy_code_3,
        taxonomy_license_3,
        taxonomy_state_3,
        taxonomy_code_4,
        taxonomy_license_4,
        taxonomy_state_4,
        taxonomy_code_5,
        taxonomy_license_5,
        taxonomy_state_5,
        valid_from,
        valid_to,
        is_current
    )
    SELECT
        md5(npi || v_today::text)::uuid,
        npi,
        last_name,
        first_name,
        entity_type_code,
        credential,
        sex,
        sole_proprietor,
        nullif(enumeration_date, '')::date,
        nullif(last_updated, '')::date,
        nullif(deactivation_date, '')::date,
        nullif(reactivation_date, '')::date,
        mailing_address_1,
        mailing_address_2,
        mailing_city,
        mailing_state,
        mailing_postal_code,
        mailing_telephone,
        location_address_1,
        location_address_2,
        location_city,
        location_state,
        location_postal_code,
        location_telephone,
        taxonomy_code_1,
        taxonomy_license_1,
        taxonomy_state_1,
        taxonomy_code_2,
        taxonomy_license_2,
        taxonomy_state_2,
        taxonomy_code_3,
        taxonomy_license_3,
        taxonomy_state_3,
        taxonomy_code_4,
        taxonomy_license_4,
        taxonomy_state_4,
        taxonomy_code_5,
        taxonomy_license_5,
        taxonomy_state_5,
        v_today,
        null::date,
        true
    FROM latest_nppes;

    -- purge duplicate npi records, keep only most recent
    WITH dupes AS (
        SELECT ctid,
               ROW_NUMBER() OVER (
                   PARTITION BY data->>'NPI'
                   ORDER BY loaded_at DESC
               ) AS rn
        FROM raw.nppes_bulk
    )
    DELETE FROM raw.nppes_bulk
    WHERE ctid IN (SELECT ctid FROM dupes WHERE rn > 1);

    RAISE NOTICE 'update_dim_provider complete';

END;
$$;
