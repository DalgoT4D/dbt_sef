SELECT
  event_type,
  evaluation_context_type,
  ai_vendor,
  ai_model,
  session_id,
  prompt_tokens_count,
  completion_tokens_count,
  total_tokens_count,
  chat_uuid,
  inserted_at,

  -- http_response fields
  JSON_EXTRACT_SCALAR(data, '$.journey_name') AS journey_name,
  JSON_VALUE(data, '$.journey_block_name') AS journey_block_name,

  -- request params (developer prompt / instructions)
  REGEXP_EXTRACT(
    JSON_VALUE(data, '$.request_params.input[0].content'),
    r'<instructions>([\s\S]*?)</instructions>'
  ) AS final_prompt,

  JSON_VALUE(
    data,
    '$.request_params.instructions'
  ) AS agent_instructions,

  -- identified queries (array → single cell)
  ARRAY_TO_STRING(
    JSON_VALUE_ARRAY(
      data,
      '$.generated_messages[0].queries'
    ),
    ';\n '
  ) AS identified_queries,

  -- final model response
  JSON_VALUE(
    data,
    '$.generated_messages[1].content[0].text.value'
  ) AS final_response

FROM {{ source('sef_whatsapp_bot', 'ai_events') }}
WHERE event_type = 'turn.run.end'
