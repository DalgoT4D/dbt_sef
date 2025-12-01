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
    JSON_EXTRACT_SCALAR(interactive, '$.body.text') AS interactive_body_text,
    inserted_at
  FROM {{ source('sef_whatsapp_bot', 'messages') }}
)

SELECT
  id,
  message_type,
  content,
  direction,
  chat_id,
  author_journey_uuid,
  author_journey_name,
  author_id,
  author_type,
  interactive_body_text,
  inserted_at,

  -- unified author name (choose stack.name or owner.journey_name)
  CASE
    WHEN author_type = 'STACK' THEN author_name_stack
    WHEN author_type = 'OWNER' THEN author_journey_name
    ELSE NULL
  END AS journey_name,

  -- journey_start_flag (trimmed safe exact matches)
  CASE
    WHEN TRIM(COALESCE(interactive_body_text, '')) IN (
      -- teaching_support_prod
      'Please tell me the grade you want support for:',
      'कृपया बताइए कि आपको किस कक्षा के लिए सहायता चाहिए:',

      -- reflection_journey_prod
      'What would you like to discuss today?',
      'आप आज किस बारे में बात करना चाहते हैं?',

      -- general_prod
      'Hello, How can I support your teaching and learning journey?',
      'नमस्ते! मैं आपकी शिक्षण और अधिगम यात्रा में कैसे मदद कर सकता/सकती हूँ?'
    )
    THEN TRUE
    ELSE FALSE
  END AS journey_start_flag

FROM base
