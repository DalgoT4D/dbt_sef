select 
    id,
    message_type,
    content,
    direction,
    chat_id,
    JSON_EXTRACT_SCALAR(author, '$.journey_uuid') AS journey_uuid,
    JSON_EXTRACT_SCALAR(author, '$.journey_name') AS journey_name,
    JSON_EXTRACT_SCALAR(author, '$.id') AS author_id,
    JSON_EXTRACT_SCALAR(author, '$.name') AS author_name,
    JSON_EXTRACT_SCALAR(author, '$.type') AS author_type,
    JSON_EXTRACT_SCALAR(interactive, '$.body.text') AS interactive_body_text,
    inserted_at
from {{ source('sef_whatsapp_bot', 'messages') }}