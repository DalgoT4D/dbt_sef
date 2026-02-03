WITH base AS (
  SELECT
    id,
    message_type,
    content,
    direction,
    chat_id,
    JSON_EXTRACT_SCALAR(author, '$.journey_uuid')   AS author_journey_uuid,
    JSON_EXTRACT_SCALAR(author, '$.journey_name')   AS author_journey_name,
    JSON_EXTRACT_SCALAR(author, '$.id')             AS author_id,
    JSON_EXTRACT_SCALAR(author, '$.name')           AS author_name_stack,
    JSON_EXTRACT_SCALAR(author, '$.type')           AS author_type,
    JSON_EXTRACT_SCALAR(author, '$.session_id')           AS session_id,
    JSON_EXTRACT_SCALAR(interactive, '$.body.text') AS interactive_body_text,
    JSON_EXTRACT_SCALAR(interactive, '$.type') AS interaction_type,
    CASE
      WHEN JSON_EXTRACT_SCALAR(interactive, '$.type') = 'button_reply' THEN
        JSON_EXTRACT_SCALAR(interactive, '$.button_reply.title')

      WHEN JSON_EXTRACT_SCALAR(interactive, '$.type') = 'list_reply' THEN
        JSON_EXTRACT_SCALAR(interactive, '$.list_reply.title')

      ELSE NULL
    END AS interaction_inbound_reply,
    inserted_at,
    ROW_NUMBER() OVER (
      PARTITION BY id
      order by inserted_at DESC
    ) as rn
  FROM {{ source('sef_whatsapp_bot', 'messages') }}
)

SELECT
  id,
  message_type,
  COALESCE(content, interactive_body_text, interaction_inbound_reply) as content_coalesced,
  interaction_type,
  direction,
  chat_id,
  author_journey_uuid,
  author_journey_name,
  author_id,
  author_type,
  session_id,
  -- interactive_body_text,
  -- interaction_inbound_reply,
  inserted_at,

  -- unified author name (choose stack.name or owner.journey_name)
  CASE
    WHEN author_type = 'STACK' THEN author_name_stack
    WHEN author_type = 'OWNER' THEN author_journey_name
    ELSE NULL
  END AS journey_name,

  -- journey_start_flag (trimmed safe exact matches)
  -- teaching_support_prod
  -- reflection_journey_prod
  -- general_prod
  CASE
    WHEN TRIM(COALESCE(interactive_body_text, '')) IN (
      'Please tell me the grade you want support for:',
      'कृपया बताएं कि आपको किस कक्षा के लिए सहायता चाहिए:',
      'What would you like to discuss today?',
      'आप आज किस बारे में बात करना चाहते हैं?',
      'Thank you for choosing to reflect today.',
      'आज चिंतन करने का विकल्प चुनने के लिए धन्यवाद।',
      'Hello, how can I support your teaching and learning journey?',
      'नमस्ते! मैं आपकी शिक्षण और अधिगम यात्रा में कैसे मदद कर सकता/सकती हूँ?'
    )
    THEN TRUE
    ELSE FALSE
  END AS journey_start_flag,

  -- Journey end flag
  CASE
    WHEN TRIM(COALESCE(interactive_body_text, '')) IN (
      -- Teaching Support prod
      'Was this suggestion useful for your class situation?',
      'क्या यह सुझाव आपकी कक्षा के लिए उपयोगी रहा?',

      -- reflection journey prod
      'Hope you have a great rest of the day!🪻', -- old journey
      'आपका दिन शानदार रहे! 🪻',
      'आशा है आपका दिन आगे अच्छा गुज़रे! 🪻' --lates (english not changed)
    )
    THEN TRUE
    ELSE FALSE
  END AS journey_end_flag,
  rn

FROM base
where rn = 1
