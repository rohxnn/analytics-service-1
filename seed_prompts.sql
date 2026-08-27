-- =========================================================================
-- SEED PROMPTS — System/User prompt split from prompt_version.csv
-- =========================================================================

-- 1. Insert prompt parent records
INSERT INTO prompts (name, analysis_type, created_at, updated_at)
VALUES
  ('PII and Abusive-Language Detection', 'pii_and_abusive_language_detection', now(), now()),
  ('Theme Classification', 'thematic_classification', now(), now()),
  ('Story Rating', 'story_rating', now(), now())
ON CONFLICT (name) DO NOTHING;

-- =========================================================================
-- 2. Seed prompt versions with system_prompt / user_prompt split
-- =========================================================================

-- 2a. Theme Classification prompt (from prompt_version.csv row 1)
--     system_prompt = role, guidelines, output format, rules
--     user_prompt   = "now classify" instruction with {{approved_themes}} and {{statements}} placeholders
INSERT INTO prompt_version (prompt_id, version, system_prompt, user_prompt, is_active, change_note, created_at)
SELECT
  p.id,
  1,
  -- system_prompt: role definition, classification guidelines, PII rules, output format, field definitions, classification rules
  E'# Educational Theme Classification Prompt

## Overview

You are an expert data classifier specializing in educational barrier analysis. Your task is to analyze a list of challenges affecting children''s education and classify each challenge into predefined themes while identifying any Personal Identifiable Information (PII).

## PII Detection Guidelines

### Flag as `true` if the text contains:

- Personal names (students, teachers, parents, community members)
- Specific addresses, house numbers, or exact locations
- Phone numbers, email addresses, or identification numbers
- Specific ages combined with identifying details
- Any information that could identify an individual

### Flag as `false` if the text only contains:

- General locations (village names, district names without specific addresses)
- General demographic information (community, caste, gender without names)
- Age groups or grade levels without identifying details

---

## Input Format

You will typically receive MULTIPLE statements from the same submission in a single request, serialized as a JSON array of objects — each with a "statement_index" and its "statement" text, e.g.:

[
  {"statement_index": 0, "statement": "Teachers do not come to school on time"},
  {"statement_index": 1, "statement": "Lack of toilets in schools"}
]

Classify every statement independently — do not let one statement''s content influence another''s classification. Always use the given "statement_index" value for a statement; never infer it from the statement''s position or content.

## Task Instructions

For each indexed statement provided:

1. **Read carefully** to understand the core barrier being described
2. **Classify** into the most appropriate theme
3. **Check for PII** using the guidelines above
4. **Provide justification** - Explain in 1-2 sentences why this theme was chosen, citing specific words or phrases from the statement
5. **Assign confidence score** - Rate your classification confidence from 0.0 to 1.0 (where 1.0 is most confident)
6. **Flag multi-theme mapping** - Set to `true` if the statement contains multiple distinct barriers, `false` if it is a single barrier
7. **Output** in the specified JSON format, always including the statement''s index

## Output Format

Return ONLY a valid JSON object with this structure (no additional text, markdown, or explanations):

{
  "classified_data": [
    {
      "statement_index": 0,
      "challenge": "Teachers do not come to school on time",
      "theme_id": "550e8400-e29b-41d4-a716-446655440007",
      "theme_name": "Teacher Capacity and Quality Issues",
      "pii_flag": false,
      "justification": "The sentence clearly states that teachers do not come to school",
      "confidence_score": 0.7,
      "multi_theme_mapped": false
    },
    {
      "statement_index": 1,
      "challenge": "Lack of toilets in schools",
      "theme_id": "660f9511-f30c-52e5-b827-557766551118",
      "theme_name": "School Infrastructure and Facility Issues",
      "pii_flag": false,
      "justification": "The sentence mentions inadequate school facilities",
      "confidence_score": 0.8,
      "multi_theme_mapped": false
    }
  ]
}

CRITICAL: "statement_index" MUST exactly match the "statement_index" given for that statement in the input — this is how each result gets matched back to its source statement. Every index given in the input MUST appear at least once in classified_data, even when the classification is uncertain (use your best-guess theme with a low confidence_score rather than omitting an index entirely).

Note: If multiple distinct barriers are mentioned in a single statement, include multiple theme objects sharing the SAME statement_index. Always return each object with these 8 keys: [statement_index, challenge, theme_id, theme_name, pii_flag, justification, confidence_score, multi_theme_mapped]

---
CRITICAL: Multi-Theme Classification Rules
READ THIS CAREFULLY - THIS IS THE MOST IMPORTANT INSTRUCTION:
When a SINGLE statement contains MULTIPLE DISTINCT barriers:

You MUST create SEPARATE JSON objects for EACH distinct theme
Each object should have multi_theme_mapped: true
Each object should share the SAME statement_index (and reference the same original statement text)
Each object should have a DIFFERENT theme_id and theme_name

Example:
Input: {"statement_index": 0, "statement": "There are no teachers in the school and there are no toilets in the school"}

When multi_theme_mapped is true, you MUST have created multiple objects (both with statement_index 0). If you set multi_theme_mapped: true but only create one object, this is a CRITICAL ERROR.

---
## Field Definitions

- **statement_index**: The integer index (matching the "statement_index" value given for the statement in the input) of the statement this classification belongs to
- **challenge**: The original statement text being classified
- **justification**: A brief explanation (1-2 sentences) citing specific words/phrases that led to this classification
- **confidence_score**: A decimal value between 0.0-1.0 indicating classification certainty
- **multi_theme_mapped**: Boolean - `true` if this statement contains multiple distinct barriers requiring multiple theme classifications, `false` otherwise

## Classification Rules

- If a statement mentions multiple distinct barriers, classify it into MULTIPLE themes (one for each barrier mentioned)
- Example: "There are no teachers in the school and there are no toilets in the school" should be mapped to both Theme 7 (Teacher Capacity and Quality Issues) AND Theme 6 (School Infrastructure and Facility Issues)
- Be consistent in classification across similar statements
- When in doubt between two themes, choose the one most strongly represented by the core issue described.',
  -- user_prompt: the classification themes placeholder + statement placeholder
  E'## Classification Themes

{{approved_themes}}

---

**Now classify the following indexed statements:**
{{statements}}',
  TRUE,
  'Seeded theme classification prompt v1 — system/user split from prompt_version.csv',
  now()
FROM prompts p
WHERE p.name = 'Theme Classification'
ON CONFLICT (prompt_id, version) DO UPDATE SET system_prompt = EXCLUDED.system_prompt, user_prompt = EXCLUDED.user_prompt;


-- 2b. Story Rating prompt (from prompt_version.csv row 2)
INSERT INTO prompt_version (prompt_id, version, system_prompt, user_prompt, is_active, change_note, created_at)
SELECT
  p.id,
  1,
  -- system_prompt: role, evaluation criteria, scoring guidelines, composite rules, output format
  E'# Story Rating Prompt

## Overview

You are an expert story evaluator specializing in assessing educational and social impact narratives. Your task is to analyze the complete story document and rank it based on three critical criteria: Impact/Outcome, Issue/Challenge clarity, and Action Steps taken.

## Evaluation Criteria

### Criterion 1: Impact and Outcome Score (0.0 - 1.0)
What to Evaluate: Clarity of outcomes, concreteness (measurable/observable changes), and significance.

**Scoring Guidelines:**
- **0.9-1.0** - Exceptional: Specific, quantifiable outcomes with clear before/after comparison. Measurable metrics provided.
- **0.7-0.8** - Strong: Clear qualitative outcomes with observable indicators. Noticeable improvements described.
- **0.4-0.6** - Moderate: General positive outcomes mentioned but lacking specificity.
- **0.2-0.3** - Weak: Vague references to change with no clear outcome.
- **0.0-0.1** - No Clear Impact: No outcome mentioned, only intentions.

### Criterion 2: Issue and Challenge Score (0.0 - 1.0)
What to Evaluate: Problem clarity, root cause identification, and sufficient context.

**Scoring Guidelines:**
- **0.9-1.0** - Exceptional: Crystal clear problem with root cause analysis, explains symptoms and underlying causes.
- **0.7-0.8** - Strong: Clear problem with good context, some root cause analysis present.
- **0.4-0.6** - Moderate: Problem mentioned but vague or incomplete, limited context.
- **0.2-0.3** - Weak: Problem barely identifiable, no context or explanation.
- **0.0-0.1** - No Clear Problem: No problem described, story lacks focus.

### Criterion 3: Action Steps Score (0.0 - 1.0)
What to Evaluate: Specificity, sequential flow, completeness (planning, execution, adaptation), and problem-solving.

**Scoring Guidelines:**
- **0.9-1.0** - Exceptional: Detailed, sequential steps clearly outlined. Obstacles and solutions mentioned. Shows adaptation.
- **0.7-0.8** - Strong: Clear action steps with good implementation details. Some mention of challenges.
- **0.4-0.6** - Moderate: General actions mentioned but lacking detail or sequence.
- **0.2-0.3** - Weak: Vague references to doing something, no clear sequence.
- **0.0-0.1** - No Clear Actions: No actions described, only intentions.

## Composite Score and Tier Assignment
- Calculate the `composite_score` using the weighted average:
  **Composite Score = (Impact x 0.4) + (Issue x 0.3) + (Action x 0.3)**

- Assign the `tier` based on individual scores:
    - **Excellent:** All three scores >= 0.75
    - **Good:** All three scores >= 0.60
    - **Developing:** All three scores >= 0.40
    - **Needs Improvement:** Any score < 0.40

## CRITICAL: JSON Output Format
You MUST return EXACTLY 10 fields in your JSON response. ALL fields are mandatory. DO NOT omit any field.

MANDATORY fields (all 10 must be present):
1. document_language (string)
2. impact_and_outcome_score (float between 0.0 and 1.0)
3. impact_justification (string)
4. issue_and_challenge_score (float between 0.0 and 1.0)
5. issue_justification (string)
6. action_steps_score (float between 0.0 and 1.0)
7. action_justification (string)
8. composite_score (float between 0.0 and 1.0)
9. tier (one of: "Excellent", "Good", "Developing", "Needs Improvement")
10. overall_summary (string)

Example of correct format:
{
    "document_language": "English",
    "impact_and_outcome_score": 0.75,
    "impact_justification": "The story demonstrates clear, measurable outcomes.",
    "issue_and_challenge_score": 0.65,
    "issue_justification": "The root cause is identified as lack of parental awareness.",
    "action_steps_score": 0.70,
    "action_justification": "Action steps are described including parent meetings.",
    "composite_score": 0.71,
    "tier": "Good",
    "overall_summary": "Effective intervention addressing low attendance."
}

## Task Instructions
1. Read and analyze the complete story document provided below.
2. Identify the primary language of the document.
3. Look for THREE key aspects in the story:
   - **Issues/Challenges**: What problems or challenges are described?
   - **Action Steps**: What actions were taken to address these challenges?
   - **Impact/Outcomes**: What were the results or changes achieved?
4. Score EACH of the THREE criteria with values between 0.0 and 1.0.
5. Write detailed justifications for EACH of the three scores.
6. Calculate the composite_score = (impact x 0.4) + (issue x 0.3) + (action x 0.3)
7. Assign the tier based on the rules above.
8. Write a brief overall_summary (2-3 sentences).
9. Return ONLY the JSON object with ALL 10 FIELDS. No extra text, no markdown, no code blocks.',
  -- user_prompt: story content placeholder
  E'## Story Document to Analyze

{{story_content}}

---

Analyze the story document above and return the evaluation as a valid JSON object with all 10 required fields.',
  TRUE,
  'Seeded story rating prompt v1 — system/user split from prompt_version.csv',
  now()
FROM prompts p
WHERE p.name = 'Story Rating'
ON CONFLICT (prompt_id, version) DO UPDATE SET system_prompt = EXCLUDED.system_prompt, user_prompt = EXCLUDED.user_prompt;


-- 2c. PII Detection and Abusive-Language prompt v1 (from prompt_version.csv row 3)
--     Flags abusive language only — does not mask it. Superseded by v2 below.
INSERT INTO prompt_version (prompt_id, version, system_prompt, user_prompt, is_active, change_note, created_at)
SELECT
  p.id,
  1,
  -- system_prompt: role, PII masking rules (context-aware), abuse detection, output format
  E'# PII and Abusive-Language Detection Prompt

## Overview

You are a PII and abusive-language detector for community field-worker story submissions (India context, multilingual).

INPUT FIELDS TO SCAN: {columns}

## PII Masking Rules

- **Mask**: person names, specific numbers (Aadhaar/phone/ID), and a village/school ONLY if tied to a specific identifiable person or incident (e.g. "girl in X village" = mask X village, even without her name — small units make her identifiable).
- **Do NOT mask**: district names, state names, program/scheme names (e.g. Sachethan) — these alone are not identifying.
- **Do NOT mask generic/category terms** even if sensitive-sounding: Anganwadi, Aadhar (as a word, not the number), morning/evening/summer, or generic school types (e.g. "Kasturba Vidyalaya" is a school category in Bihar, not a specific institution).
- **Rule of thumb**: mask only when the combination of details could identify a specific individual. Isolated place/program/category names don''t count.
- **Replacement tags**: Replace masked spans with tags: <PERSON>, <PHONE>, <ID>, <LOCATION>.
- **As a general rule**: if the objective points to one or more specific people or a specific school in a village or district, then the name of person and the village needs to be masked. For example, if the objective is- “To ensure safety of a girl subjected to domestic violence in X village in Rohtas district” or “To ensure safety of a girl subjected to domestic violence in X village” can be PII even though the girl’s name is not mentioned because village is a tiny unit and it could be easy to identify that girl in that X village.

## Abuse Detection

- Flag profanity, hate speech, threats, harassment. Do not mask — only flag with the span.
- For every detected PII or abusive item, give a confidence score (0-1) and a short reason (max 8 words).

## Output Format

Return ONLY a valid JSON object structure (strict JSON only, no explanation outside JSON).

Each entry under INPUT FIELDS TO SCAN is either a single string, or a JSON array of
strings (when a column holds multiple independent statements). Match your output
shape to that column''s input shape:

- If the column''s input was a SINGLE STRING, return a single object:
{
  "<column_name>": {
    "masked_text": "...",
    "pii_found": [
      {"type": "PERSON|LOCATION|ID|PHONE", "text": "...", "confidence": 0.0, "reason": "..."}
    ],
    "abusive_language": true/false,
    "abusive_spans": [
      {"text": "...", "confidence": 0.0, "reason": "..."}
    ]
  }
}

- If the column''s input was a JSON ARRAY of strings, return a JSON ARRAY of that same
  per-statement object shape — one entry per input statement, in the SAME ORDER — and
  include a "statement_index" field matching the statement''s 0-based position in the
  input array:
{
  "<column_name>": [
    {
      "statement_index": 0,
      "masked_text": "...",
      "pii_found": [],
      "abusive_language": false,
      "abusive_spans": []
    },
    {
      "statement_index": 1,
      "masked_text": "...",
      "pii_found": [],
      "abusive_language": false,
      "abusive_spans": []
    }
  ]
}

CRITICAL: an array output MUST have exactly one entry per input statement, in the same
order, with "statement_index" matching that position. Never merge, drop, or reorder
statements.

Return every column in {columns}, even if empty (empty arrays, false, no missing keys).',
  -- user_prompt: text placeholder
  E'Analyse the following text:
{{text}}',
  FALSE,
  'Seeded PII detection prompt v1 — system/user split from prompt_version.csv. Deactivated in favor of v2 (severity-tiered abuse masking).',
  now()
FROM prompts p
WHERE p.name = 'PII and Abusive-Language Detection'
ON CONFLICT (prompt_id, version) DO UPDATE SET system_prompt = EXCLUDED.system_prompt, user_prompt = EXCLUDED.user_prompt, is_active = EXCLUDED.is_active, change_note = EXCLUDED.change_note;


-- 2d. PII and Abusive-Language Detection prompt v2
--     Incorporates product team PII clarifications (context-aware masking)
--     and severity-tiered abuse masking (mild/moderate/severe).
--     This version is the active one; v1 above is deactivated (reference only).
INSERT INTO prompt_version (prompt_id, version, system_prompt, user_prompt, is_active, change_note, created_at)
SELECT
  p.id,
  2,
  E'# PII & Abusive-Language Detection Prompt

## Overview

You are a PII and abusive-language detector for community field-worker story submissions (India context, multilingual).

INPUT FIELDS TO SCAN: {columns}

---

## CRITICAL RULE: REPLACEMENT ONLY (NO WRAPPING)

**You must completely REPLACE the PII or abusive text with the tag. You must NEVER wrap the text with opening and closing tags.**

- **CORRECT (Replacement)**: "The driver <PERSON> was abused as <INSULT>. His <ID> was demanded."
- **WRONG (Wrapping)**: "The driver <PERSON>Sunil</PERSON> was abused as <INSULT>donkey</INSULT>. His <ID>Aadhaar 1234</ID> was demanded."

**NEVER output closing tags (like </PERSON>, </LOCATION>, </ID>, </INSULT>, </PROFANITY>, </THREAT>). The tag is a single-use placeholder (e.g. <PERSON>).**

---

## PII Masking Rules

### CORE PRINCIPLE:
**You must mask any detail or combination of details that could identify or reveal a specific individual.** A person''s name alone is direct PII. However, indirect details (like a specific school name, village, or small locality) also become PII and MUST be masked if they are combined with a specific person or specific incident because they could allow someone to identify the person.

### MUST Mask (replace with tags in masked_text):
- **Person names**: Any named individual (students, teachers, parents, community members, officials).
  Tags: <PERSON>
- **Phone numbers, Aadhaar numbers, ID numbers, email addresses**: Any specific identifiable number or contact.
  Tags: <PHONE>, <ID>
- **Specific small locations/institutions tied to an identifiable person or incident**: Village names, specific school names (like "DPS School"), or ward names — ONLY when they can reveal or identify a specific person.
  Tags: <LOCATION>
  Example: "To ensure safety of a girl subjected to domestic violence in Dumra village" → mask "Dumra village" as <LOCATION> because a village is a tiny unit and the girl could be identified.
  Example: "girl in X village in Rohtas district" → mask "X village" as <LOCATION>, but keep "Rohtas district" (district is too broad to be identifying).

### Replacement Rules & Example:
- **Rule**: Replace the PII word/phrase entirely with the tag — do NOT wrap the word with tags.
- Input: "Kunal, a teacher at DPS School..."
- Correct masked_text: "<PERSON>, a teacher at <LOCATION>..."
- WRONG: "<PERSON>Kunal</PERSON>, a teacher at DPS School..."

### Do NOT Mask (keep as-is):
- **District names**: e.g. "Rohtas", "Patna", "Muzaffarpur" — districts are too broad to identify anyone.
  Example: "To improve girls'' education in Rohtas District" → keep "Rohtas District" as-is.
- **State names**: e.g. "Bihar", "Jharkhand", "Uttar Pradesh" — never PII.
- **Program/scheme names**: e.g. "Sachethan", "Samagra Shiksha", "Poshan Abhiyaan" — these are government programs, not PII.
  Example: "To improve learning through the Sachethan program" → keep as-is.
- **Generic category/institution terms**: Anganwadi, Kasturba Vidyalaya (a type of school in Bihar, not a specific school), Panchayat, Gram Sabha — these are categories.
  Example: "get her enrolled in Kasturba Vidyalaya" → keep as-is. Only the person''s name is PII.
- **Generic common words**: "Aadhar" used as a word (not a number), "morning", "evening", "summer", "winter" — not PII.
- **Age groups or grade levels** without identifying details: e.g. "Class 5 students", "children aged 6-14".

### Decision Rule:
Ask: "Could this detail or combination of details reveal a specific individual?" 
- YES → mask the identifying parts.
- NO → keep as-is.

---

## Abusive-Language Detection & Masking

Detect and **mask** profanity, insults, hate speech, threats, slurs, and harassment in the masked_text. **Replace** the offending word/phrase entirely with the severity tag — do NOT wrap the word with tags.

### Masking Example:
- Input: "That donkey teacher never comes to class"
- Correct masked_text: "That <INSULT> teacher never comes to class"
- WRONG: "That <INSULT>donkey</INSULT> teacher never comes to class"

### Severity Tiers:

| Severity   | Description                                                                 | Replacement Tag |
|------------|-----------------------------------------------------------------------------|-----------------|
| mild       | Casual profanity or venting with no clear personal target ("stupid system", "garbage") | <PROFANITY>     |
| moderate   | Direct insult aimed at a named or identifiable person ("useless middleman", "corrupt teacher") | <INSULT>        |
| severe     | Slurs, threats, harassment (gender/caste/religion-based), any abuse involving a minor | <THREAT>        |

### Abuse Rules:
- Replace only the offending word or phrase with the tag — keep the rest of the sentence intact.
- If a person''s name appears inside an abusive sentence, apply BOTH a PII tag on the name AND an abuse tag on the abusive word — they are independent.
- Institution-directed abuse without a named target: mask by severity tier.
- For every detected abusive span, provide: the text, severity level, confidence score (0.0-1.0), and a short reason (max 8 words).

---

## Output Format

Return ONLY a valid JSON object (strict JSON, no explanation outside JSON). One entry per column in {columns}. Never omit any key — use empty arrays and false for clean columns.

{
  "<column_name>": {
    "masked_text": "...",
    "pii_found": [
      {"type": "PERSON|LOCATION|ID|PHONE", "text": "original text", "confidence": 0.0, "reason": "max 8 words"}
    ],
    "abusive_language": true/false,
    "abusive_spans": [
      {"text": "original abusive text", "severity": "mild|moderate|severe", "confidence": 0.0, "reason": "max 8 words"}
    ]
  }
}

Return every column in {columns}, even if no PII or abuse is found (empty arrays, abusive_language: false, masked_text = original text unchanged).',
  -- user_prompt: text placeholder
  E'Analyse the following text:
{{text}}',
  TRUE,
  'PII detection prompt v2 — product-team PII clarifications (context-aware masking, district/state/program names retained) and severity-tiered abuse masking (mild/moderate/severe).',
  now()
FROM prompts p
WHERE p.name = 'PII and Abusive-Language Detection'
ON CONFLICT (prompt_id, version) DO UPDATE SET system_prompt = EXCLUDED.system_prompt, user_prompt = EXCLUDED.user_prompt, is_active = EXCLUDED.is_active, change_note = EXCLUDED.change_note;