select
    id,
    uuid,
    is_deleted,
    urn,
    JSON_EXTRACT_SCALAR(details, '$.name') AS full_name,
    JSON_EXTRACT_SCALAR(details, '$.language_select') AS full_name,
    JSON_EXTRACT_SCALAR(details, '$.designation') AS designation,
    JSON_EXTRACT_SCALAR(details, '$.state') AS state,
    JSON_EXTRACT_SCALAR(details, '$.grade') AS grade,
    JSON_EXTRACT_SCALAR(details, '$.grades') AS grades,
    JSON_EXTRACT_SCALAR(details, '$.whatsapp_id') AS whatsapp_id,
    JSON_EXTRACT_SCALAR(details, '$.whatsapp_profile_name') AS whatsapp_profile_name,
    JSON_EXTRACT_SCALAR(details, '$.opted_in') AS opted_in,
    inserted_at,
    updated_at
from {{ source('sef_whatsapp_bot', 'contacts') }}
where is_deleted is false