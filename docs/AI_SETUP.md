# Configure Groq

The unified table assistant sends column names, up to eight representative sample rows, the instruction, and recent conversation to the configured provider. See [Table assistant](TABLE_ASSISTANT.md).

1. Create a key at https://console.groq.com/keys. Keep your intended plan; current quota is shown in the provider console.
2. Run `python3 scripts/configure_groq.py` from the project directory and paste your key into the hidden local input. Never share it in chat or upload it to GitHub.
3. Run `docker compose up -d --force-recreate backend worker`.
4. Open http://localhost:8080, load data, and enter an instruction.

Configuration is stored in the private local `.env`:

```dotenv
LLM_PROVIDER=groq
GROQ_API_KEY=your_key_here
GROQ_MODEL=openai/gpt-oss-20b
```

The unified assistant requires a live model. `DEMO_USE_LIVE_AI` is only for compatibility with the legacy API and does not affect this conversation interface. New AI explanations are in English; instructions and table values can be multilingual.

Run the synthetic checks:

```bash
docker compose exec worker python check_ai.py
python3 scripts/chat_smoke_test.py --url http://localhost:8080
```

Requests use your configured provider quota. Rate-limited tasks wait and retry; there is no automatic plan upgrade or paid OpenAI fallback. `openai/gpt-oss-20b` is the model name; requests go to Groq and use your Groq account quota.

Official references:

- https://console.groq.com/docs/rate-limits
- https://console.groq.com/docs/openai
- https://console.groq.com/docs/structured-outputs
- https://console.groq.com/docs/billing-faqs
