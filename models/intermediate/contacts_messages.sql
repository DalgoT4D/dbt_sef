select

    contacts.urn,
    contacts.full_name,
    contacts.designation,
    messages.content,
    messages.direction,
    messages.journey_name,
    messages.author_type,
    contacts.inserted_at as contact_inserted_at,
    messages.inserted_at as message_inserted_at,
    messages.id as message_id


 from {{ ref('contacts_flatten') }} as contacts
left join {{ ref('messages_flatten') }} as messages on contacts.whatsapp_id = messages.author_id