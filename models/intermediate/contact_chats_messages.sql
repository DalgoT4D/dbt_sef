-- For each chat get all the messages


select

    contact_chats.id as contact_id,
    contact_chats.urn,
    COALESCE(contact_chats.full_name, contact_chats.title) AS name_or_title,
    contact_chats.designation,
    contact_chats.contact_inserted_at,
    contact_chats.opted_in,
    contact_chats.chat_id,
    contact_chats.owner,
    contact_chats.inserted_at as chat_inserted_at,
    contact_chats.updated_at as chat_updated_at,

    messages.message_type,
    messages.interaction_type,
    messages.content_coalesced,
    messages.direction,
    messages.journey_name,
    messages.author_type,
    messages.journey_start_flag,
    messages.journey_end_flag,
    messages.id as message_id,
    messages.inserted_at as message_inserted_at,


from {{ ref('contact_chats') }} as contact_chats


left join {{ ref('messages_flatten') }} as messages 
on contact_chats.chat_id = messages.chat_id

where urn NOT IN ('+918168594706',
            '+917983447375',
            '+918348332976',
            '+917678621883',
            '+919606417374',
            '+918447821214',
            '+919313285427',
            '+919560681899',
            '+918052006633')




-- Check the journey start message content and the count of it (How many times this journey was triggered)