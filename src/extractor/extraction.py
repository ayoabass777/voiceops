"""
LLM extraction module (v1).

Layer 1: Structured output prompt with constrained fields.
Layer 2: validate_extraction — schema check before Postgres write.
Layer 3: Confidence-gated aggregation (dbt).
"""

import json
import logging
import os
import random
import time

logger = logging.getLogger("voiceops.extractor.llm")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

VALID_INTENTS = {
    "booking", "complaint", "inquiry",
    "emergency", "cancellation", "follow_up", "other",
}

EXTRACTION_PROMPT = """You are a call analysis engine for a service business voice AI system.
Given a phone call transcript, extract structured data.

Return ONLY valid JSON with these exact fields:
{
  "intent": one of ["booking", "complaint", "inquiry", "emergency", "cancellation", "follow_up", "other"],
  "sentiment_score": float between 1.0 and 5.0 (1=very negative, 3=neutral, 5=very positive),
  "resolved": boolean (was the customer's issue addressed in this call?),
  "summary": string max 500 chars (one-line summary of the call),
  "confidence": float between 0.0 and 1.0 (your confidence in the extraction),
  "entities": object with relevant entities like service_type, urgency, vehicle_make, unit_number, date_mentioned, cost_mentioned
}

Rules:
- confidence 0.9+ means clear intent and sentiment. Below 0.7 means ambiguous.
- If the transcript is unclear or too short (<10 words), set confidence below 0.5.
- Do not infer information not present in the transcript.
- Do not hallucinate entities. Only include what is explicitly mentioned.
- Return ONLY the JSON object. No markdown, no backticks, no explanation."""


def extract_with_llm(transcript: str) -> tuple[dict, int]:
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)

    start = time.time()
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": EXTRACTION_PROMPT},
            {"role": "user", "content": f"Transcript:\n{transcript}"},
        ],
        temperature=0.1,
        max_tokens=500,
        response_format={"type": "json_object"},
    )
    latency_ms = int((time.time() - start) * 1000)
    raw_text = response.choices[0].message.content.strip()
    return json.loads(raw_text), latency_ms


def extract_with_mock(transcript: str) -> tuple[dict, int]:
    start = time.time()
    text_lower = transcript.lower()

    if any(w in text_lower for w in ["emergency", "flooding", "sewage", "rising", "water is rising"]):
        intent = "emergency"
    elif any(w in text_lower for w in ["cancel", "cancellation"]):
        intent = "cancellation"
    elif any(w in text_lower for w in ["frustrat", "refund", "not working", "grinding", "third time",
                                          "unacceptable", "nobody showed", "nobody has come", "complaint"]):
        intent = "complaint"
    elif any(w in text_lower for w in ["book", "schedule", "appointment", "slot", "set for"]):
        intent = "booking"
    elif any(w in text_lower for w in ["status", "check on", "follow up", "call back"]):
        intent = "follow_up"
    elif any(w in text_lower for w in ["how much", "quote", "do you", "looking for"]):
        intent = "inquiry"
    else:
        intent = "other"

    neg = ["frustrat", "angry", "unacceptable", "terrible", "refund", "third time",
           "nobody showed", "nobody has come"]
    pos = ["thanks", "great", "perfect", "reasonable", "appreciate", "good"]
    neg_count = sum(1 for w in neg if w in text_lower)
    pos_count = sum(1 for w in pos if w in text_lower)

    if neg_count > pos_count:
        sentiment = round(random.uniform(1.2, 2.5), 1)
    elif pos_count > neg_count:
        sentiment = round(random.uniform(3.5, 4.8), 1)
    else:
        sentiment = round(random.uniform(2.8, 3.5), 1)

    resolved = any(w in text_lower for w in [
        "booked", "confirmed", "scheduled", "dispatching",
        "cancelled", "you're all set", "you're confirmed",
        "will be out", "on their way", "reschedule",
        "escalating", "priority one", "logging this as urgent",
        "i'll have an answer", "we'll call you",
        "someone will be", "i'll book",
    ])

    word_count = len(transcript.split())
    summary = f"{intent.replace('_', ' ').title()} call ({word_count} words). "
    summary += "Issue addressed." if resolved else "Pending follow-up."

    entities = {}
    if any(w in text_lower for w in ["leak", "drain", "plumbing", "pipe", "water"]):
        entities["service_type"] = "plumbing"
    elif any(w in text_lower for w in ["oil change", "brake", "transmission", "tire"]):
        entities["service_type"] = "auto_repair"
    elif any(w in text_lower for w in ["unit", "lease", "rent", "tenant", "maintenance"]):
        entities["service_type"] = "property_management"
    if intent == "emergency":
        entities["urgency"] = "high"
    elif intent == "complaint":
        entities["urgency"] = "medium"

    confidence = round(random.uniform(0.75, 0.95), 2)
    if word_count < 50:
        confidence = round(random.uniform(0.4, 0.65), 2)

    latency_ms = int((time.time() - start) * 1000) + random.randint(100, 500)

    return {
        "intent": intent,
        "sentiment_score": sentiment,
        "resolved": resolved,
        "summary": summary[:500],
        "confidence": confidence,
        "entities": entities,
    }, latency_ms


def extract(transcript: str) -> tuple[dict, str, int]:
    if OPENAI_API_KEY:
        extraction, latency = extract_with_llm(transcript)
        return extraction, LLM_MODEL, latency
    else:
        extraction, latency = extract_with_mock(transcript)
        return extraction, "mock-v1", latency


def validate_extraction(data: dict) -> tuple[bool, str]:
    """Layer 2 guardrail: schema validation before Postgres write."""
    if data.get("intent") not in VALID_INTENTS:
        return False, f"invalid intent: {data.get('intent')}"
    sentiment = data.get("sentiment_score", 0)
    if not isinstance(sentiment, (int, float)) or not (1.0 <= sentiment <= 5.0):
        return False, f"sentiment out of range: {sentiment}"
    if not isinstance(data.get("resolved"), bool):
        return False, f"resolved not boolean: {data.get('resolved')}"
    summary = data.get("summary", "")
    if not isinstance(summary, str) or len(summary) > 500:
        return False, f"summary invalid: {len(summary)} chars"
    confidence = data.get("confidence", -1)
    if not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
        return False, f"confidence out of range: {confidence}"
    if not isinstance(data.get("entities", {}), dict):
        return False, "entities not a dict"
    return True, ""
