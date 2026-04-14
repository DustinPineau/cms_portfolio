{{ config(materialized='table', tags=['weekly']) }}

select
    data->>'NPI'                                                                        as npi,
    data->>'Entity Type Code'                                                           as entity_type_code,
    data->>'Provider Last Name (Legal Name)'                                            as last_name,
    data->>'Provider First Name'                                                        as first_name,
    data->>'Provider Middle Name'                                                       as middle_name,
    data->>'Provider Name Prefix Text'                                                  as name_prefix,
    data->>'Provider Name Suffix Text'                                                  as name_suffix,
    data->>'Provider Credential Text'                                                   as credential,
    data->>'Provider Other Organization Name'                                           as other_organization_name,
    data->>'Provider Other Organization Name Type Code'                                 as other_organization_name_type_code,
    data->>'Provider Organization Name (Legal Business Name)'                           as organization_name,
    data->>'Provider Sex Code'                                                          as sex,
    data->>'Is Sole Proprietor'                                                         as sole_proprietor,
    data->>'Is Organization Subpart'                                                    as is_organization_subpart,

    -- dates
    case when data->>'Provider Enumeration Date' = '' then null
         else to_date(data->>'Provider Enumeration Date', 'MM/DD/YYYY') end             as enumeration_date,
    case when data->>'Last Update Date' = '' then null
         else to_date(data->>'Last Update Date', 'MM/DD/YYYY') end                      as last_updated,
    case when data->>'NPI Deactivation Date' = '' then null
         else to_date(data->>'NPI Deactivation Date', 'MM/DD/YYYY') end                 as deactivation_date,
    case when data->>'NPI Reactivation Date' = '' then null
         else to_date(data->>'NPI Reactivation Date', 'MM/DD/YYYY') end                 as reactivation_date,
    case when data->>'Certification Date' = '' then null
         else to_date(data->>'Certification Date', 'MM/DD/YYYY') end                    as certification_date,

    -- mailing address
    data->>'Provider First Line Business Mailing Address'                               as mailing_address_1,
    data->>'Provider Second Line Business Mailing Address'                              as mailing_address_2,
    data->>'Provider Business Mailing Address City Name'                                as mailing_city,
    data->>'Provider Business Mailing Address State Name'                               as mailing_state,
    data->>'Provider Business Mailing Address Postal Code'                              as mailing_postal_code,
    data->>'Provider Business Mailing Address Telephone Number'                         as mailing_telephone,
    data->>'Provider Business Mailing Address Fax Number'                               as mailing_fax,
    data->>'Provider Business Mailing Address Country Code (If outside U.S.)'           as mailing_country_code,

    -- practice location address
    data->>'Provider First Line Business Practice Location Address'                     as location_address_1,
    data->>'Provider Second Line Business Practice Location Address'                    as location_address_2,
    data->>'Provider Business Practice Location Address City Name'                      as location_city,
    data->>'Provider Business Practice Location Address State Name'                     as location_state,
    data->>'Provider Business Practice Location Address Postal Code'                    as location_postal_code,
    data->>'Provider Business Practice Location Address Telephone Number'               as location_telephone,
    data->>'Provider Business Practice Location Address Fax Number'                     as location_fax,
    data->>'Provider Business Practice Location Address Country Code (If outside U.S.)' as location_country_code,

    -- taxonomies
    data->>'Healthcare Provider Taxonomy Code_1'                                        as taxonomy_code_1,
    data->>'Provider License Number_1'                                                  as taxonomy_license_1,
    data->>'Provider License Number State Code_1'                                       as taxonomy_state_1,
    data->>'Healthcare Provider Primary Taxonomy Switch_1'                              as taxonomy_primary_1,
    data->>'Healthcare Provider Taxonomy Code_2'                                        as taxonomy_code_2,
    data->>'Provider License Number_2'                                                  as taxonomy_license_2,
    data->>'Provider License Number State Code_2'                                       as taxonomy_state_2,
    data->>'Healthcare Provider Primary Taxonomy Switch_2'                              as taxonomy_primary_2,
    data->>'Healthcare Provider Taxonomy Code_3'                                        as taxonomy_code_3,
    data->>'Provider License Number_3'                                                  as taxonomy_license_3,
    data->>'Provider License Number State Code_3'                                       as taxonomy_state_3,
    data->>'Healthcare Provider Primary Taxonomy Switch_3'                              as taxonomy_primary_3,
    data->>'Healthcare Provider Taxonomy Code_4'                                        as taxonomy_code_4,
    data->>'Provider License Number_4'                                                  as taxonomy_license_4,
    data->>'Provider License Number State Code_4'                                       as taxonomy_state_4,
    data->>'Healthcare Provider Primary Taxonomy Switch_4'                              as taxonomy_primary_4,
    data->>'Healthcare Provider Taxonomy Code_5'                                        as taxonomy_code_5,
    data->>'Provider License Number_5'                                                  as taxonomy_license_5,
    data->>'Provider License Number State Code_5'                                       as taxonomy_state_5,
    data->>'Healthcare Provider Primary Taxonomy Switch_5'                              as taxonomy_primary_5,

    loaded_at,
    'weekly'::text                                                                        as source

from raw.nppes_weekly

