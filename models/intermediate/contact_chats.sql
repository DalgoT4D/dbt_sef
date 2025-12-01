-- For each contact get all the chats - This model

select

    contacts.id,
    contacts.urn,
    contacts.full_name,
    contacts.designation,
    contacts.inserted_at as contact_inserted_at,
    contacts.opted_in,

    chats.id as chat_id,
    chats.owner,
    chats.state,
    chats.state_reason,
    chats.title,
    chats.contact_id,
    chats.inserted_at,
    chats.updated_at
from {{ ref('contacts_flatten') }} as contacts 
left join {{ source('sef_whatsapp_bot', 'chats') }} as chats 
on contacts.id = chats.contact_id






