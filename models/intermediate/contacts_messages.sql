select
    contacts.urn,
    contacts.full_name,
    contacts.designation,
    contacts.inserted_at as contact_inserted_at,
    contacts.opted_in,
    messages.content,
    messages.interactive_body_text,
    messages.direction,
    messages.journey_name,
    messages.author_type,
    messages.id as message_id,
    messages.inserted_at as message_inserted_at
from {{ ref('messages_flatten') }} as messages 
left join {{ ref('contacts_flatten') }} as contacts 
on contacts.whatsapp_id = messages.author_id